"""
Job Database Layer for Background Processing
Persistent storage for job metadata, status, and progress tracking
"""

import sqlite3
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from enum import Enum
import hashlib

class JobStatus(Enum):
    """Job status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class JobDatabase:
    """SQLite database for job persistence and management"""
    
    def __init__(self, db_path: str = "background_jobs.db"):
        """Initialize job database"""
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create jobs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    sheet_id TEXT NOT NULL,
                    sheet_name TEXT NOT NULL,
                    case_type TEXT NOT NULL,
                    start_row INTEGER NOT NULL,
                    num_rows INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    progress REAL DEFAULT 0.0,
                    processed_rows INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    api_key_hash TEXT,
                    metadata TEXT
                )
            """)
            
            # Create job_logs table for detailed logging
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details TEXT,
                    FOREIGN KEY (job_id) REFERENCES jobs (id)
                )
            """)
            
            # Create indexes for better performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs (created_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs (job_id)")
            
            conn.commit()
    
    def create_job(self, job_data: Dict) -> str:
        """
        Create a new job in the database
        
        Args:
            job_data: Dictionary containing job information
                - sheet_id: Google Sheets ID
                - sheet_name: Name of the sheet
                - case_type: "CASE_A" or "CASE_B"
                - start_row: Starting row number
                - num_rows: Total number of rows to process
                - api_key: OpenAI API key (will be hashed)
                - metadata: Optional additional data
        
        Returns:
            job_id: Unique job identifier
        """
        import uuid
        
        job_id = str(uuid.uuid4())
        
        # Hash the API key for security
        api_key_hash = hashlib.sha256(job_data.get('api_key', '').encode()).hexdigest()[:16]
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO jobs (
                    id, sheet_id, sheet_name, case_type, start_row, num_rows,
                    status, api_key_hash, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job_id,
                job_data['sheet_id'],
                job_data['sheet_name'],
                job_data['case_type'],
                job_data['start_row'],
                job_data['num_rows'],
                JobStatus.PENDING.value,
                api_key_hash,
                json.dumps(job_data.get('metadata', {}))
            ))
            
            conn.commit()
            
            # Log job creation
            self.log_job_event(job_id, "INFO", "Job created", {
                "sheet_id": job_data['sheet_id'],
                "case_type": job_data['case_type'],
                "num_rows": job_data['num_rows']
            })
        
        return job_id
    
    def update_job_status(self, job_id: str, status: JobStatus, 
                         progress: float = None, processed_rows: int = None,
                         error_message: str = None) -> bool:
        """
        Update job status and progress
        
        Args:
            job_id: Job identifier
            status: New job status
            progress: Progress percentage (0.0 to 1.0)
            processed_rows: Number of rows processed
            error_message: Error message if status is FAILED
        
        Returns:
            bool: True if update successful
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Build update query dynamically
                updates = ["status = ?"]
                params = [status.value]
                
                if progress is not None:
                    updates.append("progress = ?")
                    params.append(progress)
                
                if processed_rows is not None:
                    updates.append("processed_rows = ?")
                    params.append(processed_rows)
                
                if error_message is not None:
                    updates.append("error_message = ?")
                    params.append(error_message)
                
                # Set timestamps based on status
                if status == JobStatus.RUNNING:
                    updates.append("started_at = ?")
                    params.append(datetime.now().isoformat())
                elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                    updates.append("completed_at = ?")
                    params.append(datetime.now().isoformat())
                
                params.append(job_id)
                
                query = f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?"
                cursor.execute(query, params)
                
                if cursor.rowcount == 0:
                    return False
                
                conn.commit()
                
                # Log status change
                self.log_job_event(job_id, "INFO", f"Status changed to {status.value}", {
                    "progress": progress,
                    "processed_rows": processed_rows
                })
                
                return True
                
        except Exception as e:
            print(f"Error updating job status: {e}")
            return False
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get job details by ID"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            # Convert row to dictionary
            columns = [description[0] for description in cursor.description]
            job = dict(zip(columns, row))
            
            # Parse metadata JSON
            if job['metadata']:
                job['metadata'] = json.loads(job['metadata'])
            else:
                job['metadata'] = {}
            
            return job
    
    def get_jobs_by_status(self, status: JobStatus) -> List[Dict]:
        """Get all jobs with specific status"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC", (status.value,))
            rows = cursor.fetchall()
            
            columns = [description[0] for description in cursor.description]
            jobs = []
            
            for row in rows:
                job = dict(zip(columns, row))
                if job['metadata']:
                    job['metadata'] = json.loads(job['metadata'])
                else:
                    job['metadata'] = {}
                jobs.append(job)
            
            return jobs
    
    def get_all_jobs(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Get all jobs with pagination"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM jobs 
                ORDER BY created_at DESC 
                LIMIT ? OFFSET ?
            """, (limit, offset))
            rows = cursor.fetchall()
            
            columns = [description[0] for description in cursor.description]
            jobs = []
            
            for row in rows:
                job = dict(zip(columns, row))
                if job['metadata']:
                    job['metadata'] = json.loads(job['metadata'])
                else:
                    job['metadata'] = {}
                jobs.append(job)
            
            return jobs
    
    def get_job_count_by_status(self) -> Dict[str, int]:
        """Get count of jobs by status"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT status, COUNT(*) as count 
                FROM jobs 
                GROUP BY status
            """)
            rows = cursor.fetchall()
            
            counts = {}
            for status, count in rows:
                counts[status] = count
            
            return counts
    
    def log_job_event(self, job_id: str, level: str, message: str, details: Dict = None):
        """Log an event for a job"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO job_logs (job_id, level, message, details)
                    VALUES (?, ?, ?, ?)
                """, (job_id, level, message, json.dumps(details or {})))
                conn.commit()
        except Exception as e:
            print(f"Error logging job event: {e}")
    
    def get_job_logs(self, job_id: str, limit: int = 50) -> List[Dict]:
        """Get logs for a specific job"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM job_logs 
                WHERE job_id = ? 
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (job_id, limit))
            rows = cursor.fetchall()
            
            columns = [description[0] for description in cursor.description]
            logs = []
            
            for row in rows:
                log = dict(zip(columns, row))
                if log['details']:
                    log['details'] = json.loads(log['details'])
                else:
                    log['details'] = {}
                logs.append(log)
            
            return logs
    
    def delete_job(self, job_id: str) -> bool:
        """Delete a job and its logs"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Delete logs first (foreign key constraint)
                cursor.execute("DELETE FROM job_logs WHERE job_id = ?", (job_id,))
                
                # Delete job
                cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
                
                conn.commit()
                return cursor.rowcount > 0
                
        except Exception as e:
            print(f"Error deleting job: {e}")
            return False
    
    def cleanup_old_jobs(self, days_old: int = 30) -> int:
        """Clean up completed jobs older than specified days"""
        cutoff_date = datetime.now().timestamp() - (days_old * 24 * 60 * 60)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get jobs to delete
            cursor.execute("""
                SELECT id FROM jobs 
                WHERE status IN ('completed', 'failed', 'cancelled')
                AND completed_at IS NOT NULL
                AND strftime('%s', completed_at) < ?
            """, (cutoff_date,))
            
            job_ids = [row[0] for row in cursor.fetchall()]
            
            if not job_ids:
                return 0
            
            # Delete logs first
            placeholders = ','.join('?' * len(job_ids))
            cursor.execute(f"DELETE FROM job_logs WHERE job_id IN ({placeholders})", job_ids)
            
            # Delete jobs
            cursor.execute(f"DELETE FROM jobs WHERE id IN ({placeholders})", job_ids)
            
            conn.commit()
            return len(job_ids)
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get job counts by status
            cursor.execute("""
                SELECT status, COUNT(*) as count 
                FROM jobs 
                GROUP BY status
            """)
            status_counts = dict(cursor.fetchall())
            
            # Get total jobs
            cursor.execute("SELECT COUNT(*) FROM jobs")
            total_jobs = cursor.fetchone()[0]
            
            # Get database size
            cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
            db_size = cursor.fetchone()[0]
            
            return {
                "total_jobs": total_jobs,
                "status_counts": status_counts,
                "database_size_bytes": db_size,
                "database_size_mb": round(db_size / (1024 * 1024), 2)
            }
