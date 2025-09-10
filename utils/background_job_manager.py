"""
Background Job Manager for Concurrent Google Sheets Processing
Central orchestration system for managing job queue, workers, and status tracking
"""

import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timedelta
import uuid

from .job_database import JobDatabase, JobStatus
from .job_models import JobData, JobProgress, JobStats, WorkerInfo, JobQueueInfo, JobError

class BackgroundJobManager:
    """Central job management system for background processing"""
    
    def __init__(self, max_workers: int = 3, db_path: str = "background_jobs.db"):
        """
        Initialize background job manager
        
        Args:
            max_workers: Maximum number of concurrent workers
            db_path: Path to SQLite database file
        """
        self.max_workers = max_workers
        self.db = JobDatabase(db_path)
        
        # Job queue and management
        self.job_queue = queue.Queue()
        self.active_jobs: Dict[str, JobData] = {}
        self.workers: Dict[str, WorkerInfo] = {}
        
        # Threading and execution
        self.worker_pool = None
        self.manager_thread = None
        self.is_running = False
        self.shutdown_event = threading.Event()
        
        # Callbacks for progress updates
        self.progress_callbacks: List[Callable] = []
        self.status_callbacks: List[Callable] = []
        
        # Statistics
        self.stats = JobStats()
        self.last_stats_update = datetime.now()
        
        # Start the manager
        self.start()
    
    def start(self):
        """Start the background job manager"""
        if self.is_running:
            return
        
        self.is_running = True
        self.shutdown_event.clear()
        
        # Initialize worker pool
        self.worker_pool = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="BackgroundWorker"
        )
        
        # Start manager thread
        self.manager_thread = threading.Thread(
            target=self._manager_loop,
            name="JobManager",
            daemon=True
        )
        self.manager_thread.start()
        
        # Resume any pending jobs from database
        self._resume_pending_jobs()
        
        print(f"✅ Background Job Manager started with {self.max_workers} workers")
    
    def stop(self):
        """Stop the background job manager"""
        if not self.is_running:
            return
        
        print("🛑 Stopping Background Job Manager...")
        
        self.is_running = False
        self.shutdown_event.set()
        
        # Shutdown worker pool
        if self.worker_pool:
            self.worker_pool.shutdown(wait=True)
        
        # Wait for manager thread
        if self.manager_thread and self.manager_thread.is_alive():
            self.manager_thread.join(timeout=5)
        
        print("✅ Background Job Manager stopped")
    
    def create_job(self, sheet_id: str, sheet_name: str, case_type: str,
                   start_row: int, num_rows: int, api_key: str = None,
                   metadata: Dict = None) -> str:
        """
        Create and queue a new background job
        
        Args:
            sheet_id: Google Sheets ID
            sheet_name: Name of the sheet
            case_type: "CASE_A" or "CASE_B"
            start_row: Starting row number
            num_rows: Total number of rows to process
            api_key: OpenAI API key
            metadata: Optional additional data
        
        Returns:
            job_id: Unique job identifier
        """
        try:
            # Validate inputs
            if not sheet_id or not sheet_name:
                raise JobError("", "Sheet ID and name are required")
            
            if case_type not in ["CASE_A", "CASE_B"]:
                raise JobError("", "Case type must be CASE_A or CASE_B")
            
            if start_row < 1 or num_rows < 1:
                raise JobError("", "Start row and num_rows must be positive")
            
            # Create job data
            job_data = {
                'sheet_id': sheet_id,
                'sheet_name': sheet_name,
                'case_type': case_type,
                'start_row': start_row,
                'num_rows': num_rows,
                'api_key': api_key,
                'metadata': metadata or {}
            }
            
            # Store in database
            job_id = self.db.create_job(job_data)
            
            # Add to queue
            self.job_queue.put(job_id)
            
            # Update statistics
            self._update_stats()
            
            print(f"✅ Job created: {job_id} ({case_type}, {num_rows} rows)")
            return job_id
            
        except Exception as e:
            print(f"❌ Error creating job: {e}")
            raise JobError("", f"Failed to create job: {str(e)}")
    
    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get current status of a job"""
        try:
            job = self.db.get_job(job_id)
            if not job:
                return None
            
            # Add queue position if pending
            if job['status'] == JobStatus.PENDING.value:
                queue_position = self._get_queue_position(job_id)
                job['queue_position'] = queue_position
            
            return job
            
        except Exception as e:
            print(f"❌ Error getting job status: {e}")
            return None
    
    def get_all_jobs(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Get all jobs with pagination"""
        return self.db.get_all_jobs(limit, offset)
    
    def get_jobs_by_status(self, status: str) -> List[Dict]:
        """Get jobs by status"""
        try:
            job_status = JobStatus(status)
            return self.db.get_jobs_by_status(job_status)
        except ValueError:
            return []
    
    def pause_job(self, job_id: str) -> bool:
        """Pause a running job"""
        try:
            job = self.db.get_job(job_id)
            if not job:
                return False
            
            if job['status'] not in [JobStatus.RUNNING.value, JobStatus.PENDING.value]:
                return False
            
            # Update status in database
            success = self.db.update_job_status(
                job_id, 
                JobStatus.PAUSED,
                error_message="Paused by user"
            )
            
            if success:
                # Remove from active jobs if running
                if job_id in self.active_jobs:
                    del self.active_jobs[job_id]
                
                self._notify_status_change(job_id, JobStatus.PAUSED)
                print(f"⏸️ Job paused: {job_id}")
            
            return success
            
        except Exception as e:
            print(f"❌ Error pausing job: {e}")
            return False
    
    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job"""
        try:
            job = self.db.get_job(job_id)
            if not job:
                return False
            
            if job['status'] != JobStatus.PAUSED.value:
                return False
            
            # Update status to pending and re-queue
            success = self.db.update_job_status(job_id, JobStatus.PENDING)
            
            if success:
                self.job_queue.put(job_id)
                self._notify_status_change(job_id, JobStatus.PENDING)
                print(f"▶️ Job resumed: {job_id}")
            
            return success
            
        except Exception as e:
            print(f"❌ Error resuming job: {e}")
            return False
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a job"""
        try:
            job = self.db.get_job(job_id)
            if not job:
                return False
            
            if job['status'] in [JobStatus.COMPLETED.value, JobStatus.CANCELLED.value]:
                return False
            
            # Update status in database
            success = self.db.update_job_status(
                job_id,
                JobStatus.CANCELLED,
                error_message="Cancelled by user"
            )
            
            if success:
                # Remove from active jobs if running
                if job_id in self.active_jobs:
                    del self.active_jobs[job_id]
                
                self._notify_status_change(job_id, JobStatus.CANCELLED)
                print(f"❌ Job cancelled: {job_id}")
            
            return success
            
        except Exception as e:
            print(f"❌ Error cancelling job: {e}")
            return False
    
    def delete_job(self, job_id: str) -> bool:
        """Delete a job and its data"""
        try:
            # Cancel if running
            if job_id in self.active_jobs:
                self.cancel_job(job_id)
            
            # Delete from database
            success = self.db.delete_job(job_id)
            
            if success:
                self._notify_status_change(job_id, "DELETED")
                print(f"🗑️ Job deleted: {job_id}")
            
            return success
            
        except Exception as e:
            print(f"❌ Error deleting job: {e}")
            return False
    
    def get_queue_info(self) -> JobQueueInfo:
        """Get current queue information"""
        queue_size = self.job_queue.qsize()
        active_workers = len([w for w in self.workers.values() if w.status == "busy"])
        
        # Estimate wait time (rough calculation)
        estimated_wait_time = 0.0
        if queue_size > 0 and active_workers > 0:
            # Assume average processing time per job
            avg_processing_time = 5.0  # minutes per job
            estimated_wait_time = (queue_size / active_workers) * avg_processing_time
        
        return JobQueueInfo(
            queue_size=queue_size,
            estimated_wait_time=estimated_wait_time,
            active_workers=active_workers,
            max_workers=self.max_workers
        )
    
    def get_statistics(self) -> JobStats:
        """Get current job statistics"""
        self._update_stats()
        return self.stats
    
    def add_progress_callback(self, callback: Callable):
        """Add callback for progress updates"""
        self.progress_callbacks.append(callback)
    
    def add_status_callback(self, callback: Callable):
        """Add callback for status changes"""
        self.status_callbacks.append(callback)
    
    def _manager_loop(self):
        """Main manager loop for processing jobs"""
        print("🔄 Job Manager loop started")
        
        while self.is_running and not self.shutdown_event.is_set():
            try:
                # Process jobs from queue
                if not self.job_queue.empty():
                    job_id = self.job_queue.get_nowait()
                    
                    # Check if job is still valid
                    job = self.db.get_job(job_id)
                    if not job or job['status'] != JobStatus.PENDING.value:
                        continue
                    
                    # Submit to worker pool
                    future = self.worker_pool.submit(self._process_job, job_id)
                    
                    # Store future for tracking
                    self.active_jobs[job_id] = job
                
                # Update worker status
                self._update_worker_status()
                
                # Update statistics periodically
                if (datetime.now() - self.last_stats_update).seconds > 30:
                    self._update_stats()
                
                # Small delay to prevent busy waiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ Error in manager loop: {e}")
                time.sleep(1)
        
        print("🔄 Job Manager loop stopped")
    
    def _process_job(self, job_id: str):
        """Process a single job (runs in worker thread)"""
        worker_id = threading.current_thread().name
        
        try:
            # Get job data
            job = self.db.get_job(job_id)
            if not job:
                print(f"❌ Job not found: {job_id}")
                return
            
            # Update worker info
            self.workers[worker_id] = WorkerInfo(
                worker_id=worker_id,
                status="busy",
                current_job_id=job_id,
                started_at=datetime.now(),
                last_heartbeat=datetime.now()
            )
            
            # Update job status to running
            self.db.update_job_status(job_id, JobStatus.RUNNING)
            self._notify_status_change(job_id, JobStatus.RUNNING)
            
            print(f"🔄 Processing job: {job_id} ({job['case_type']}, {job['num_rows']} rows)")
            
            # Import here to avoid circular imports
            from .background_processor import BackgroundProcessor
            
            # Create processor and process job
            processor = BackgroundProcessor(self)
            success = processor.process_job(job)
            
            if success:
                # Mark as completed
                self.db.update_job_status(
                    job_id, 
                    JobStatus.COMPLETED,
                    progress=100.0,
                    processed_rows=job['num_rows']
                )
                self._notify_status_change(job_id, JobStatus.COMPLETED)
                print(f"✅ Job completed: {job_id}")
            else:
                # Mark as failed
                self.db.update_job_status(
                    job_id,
                    JobStatus.FAILED,
                    error_message="Processing failed"
                )
                self._notify_status_change(job_id, JobStatus.FAILED)
                print(f"❌ Job failed: {job_id}")
            
        except Exception as e:
            print(f"❌ Error processing job {job_id}: {e}")
            
            # Mark as failed
            self.db.update_job_status(
                job_id,
                JobStatus.FAILED,
                error_message=str(e)
            )
            self._notify_status_change(job_id, JobStatus.FAILED)
        
        finally:
            # Clean up
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]
            
            # Update worker status
            if worker_id in self.workers:
                self.workers[worker_id].status = "idle"
                self.workers[worker_id].current_job_id = None
                self.workers[worker_id].jobs_processed += 1
    
    def _resume_pending_jobs(self):
        """Resume any pending jobs from database on startup"""
        try:
            pending_jobs = self.db.get_jobs_by_status(JobStatus.PENDING)
            for job in pending_jobs:
                self.job_queue.put(job['id'])
            
            if pending_jobs:
                print(f"🔄 Resumed {len(pending_jobs)} pending jobs")
                
        except Exception as e:
            print(f"❌ Error resuming pending jobs: {e}")
    
    def _get_queue_position(self, job_id: str) -> int:
        """Get position of job in queue"""
        # This is a simplified implementation
        # In a real system, you'd want to track queue positions more precisely
        return self.job_queue.qsize()
    
    def _update_worker_status(self):
        """Update worker status and clean up dead workers"""
        current_time = datetime.now()
        
        for worker_id, worker in list(self.workers.items()):
            if not worker.is_alive():
                print(f"⚠️ Worker {worker_id} appears to be dead, cleaning up")
                if worker.current_job_id:
                    # Mark job as failed
                    self.db.update_job_status(
                        worker.current_job_id,
                        JobStatus.FAILED,
                        error_message="Worker died during processing"
                    )
                del self.workers[worker_id]
            else:
                # Update heartbeat
                worker.last_heartbeat = current_time
    
    def _update_stats(self):
        """Update job statistics"""
        try:
            status_counts = self.db.get_job_count_by_status()
            
            self.stats.total_jobs = sum(status_counts.values())
            self.stats.pending_jobs = status_counts.get(JobStatus.PENDING.value, 0)
            self.stats.running_jobs = status_counts.get(JobStatus.RUNNING.value, 0)
            self.stats.completed_jobs = status_counts.get(JobStatus.COMPLETED.value, 0)
            self.stats.failed_jobs = status_counts.get(JobStatus.FAILED.value, 0)
            self.stats.cancelled_jobs = status_counts.get(JobStatus.CANCELLED.value, 0)
            
            self.stats.success_rate = self.stats.calculate_success_rate()
            self.last_stats_update = datetime.now()
            
        except Exception as e:
            print(f"❌ Error updating stats: {e}")
    
    def _notify_status_change(self, job_id: str, status: JobStatus):
        """Notify callbacks of status changes"""
        for callback in self.status_callbacks:
            try:
                callback(job_id, status)
            except Exception as e:
                print(f"❌ Error in status callback: {e}")
    
    def _notify_progress(self, job_id: str, progress: float, message: str = ""):
        """Notify callbacks of progress updates"""
        for callback in self.progress_callbacks:
            try:
                callback(job_id, progress, message)
            except Exception as e:
                print(f"❌ Error in progress callback: {e}")
    
    def __del__(self):
        """Cleanup on destruction"""
        try:
            if hasattr(self, 'is_running') and self.is_running:
                self.stop()
        except Exception:
            # Ignore errors during cleanup
            pass
