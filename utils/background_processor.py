"""
Background Processor for Individual Job Processing
Handles the actual processing of Google Sheets jobs in background threads
"""

import time
import os
from typing import Dict, Optional, Callable
from datetime import datetime
from dotenv import load_dotenv

from .job_database import JobDatabase, JobStatus
from .job_models import JobData, JobProgress, JobError
from .google_sheets_processor_fixed import GoogleSheetsProcessor

# Load environment variables from .env file
load_dotenv()

class BackgroundProcessor:
    """Processes individual background jobs"""
    
    def __init__(self, job_manager, api_key: str = None):
        """
        Initialize background processor
        
        Args:
            job_manager: Reference to the job manager
            api_key: OpenAI API key (if not provided, will be retrieved from environment)
        """
        self.job_manager = job_manager
        self.api_key = api_key
        self.sheets_processor = None
        self.current_job = None
    
    def process_job(self, job_data: Dict) -> bool:
        """
        Process a single job
        
        Args:
            job_data: Job data from database
            
        Returns:
            bool: True if processing successful, False otherwise
        """
        job_id = job_data['id']
        self.current_job = job_data
        
        try:
            print(f"🔄 Starting job processing: {job_id}")
            
            # Get API key from environment if not provided
            if not self.api_key:
                self.api_key = os.getenv('OPENAI_API_KEY')
                if not self.api_key:
                    raise JobError(job_id, "OPENAI_API_KEY environment variable not set")
            
            # Initialize Google Sheets processor
            self.sheets_processor = GoogleSheetsProcessor(self.api_key)
            
            # Check authentication
            if not self.sheets_processor.is_authenticated():
                raise JobError(job_id, "Google Sheets authentication required")
            
            # Process the sheet
            success = self._process_sheet(job_data)
            
            if success:
                print(f"✅ Job completed successfully: {job_id}")
                return True
            else:
                print(f"❌ Job processing failed: {job_id}")
                return False
                
        except Exception as e:
            print(f"❌ Error processing job {job_id}: {e}")
            
            # Update job status to failed
            self.job_manager.db.update_job_status(
                job_id,
                JobStatus.FAILED,
                error_message=str(e)
            )
            
            return False
        
        finally:
            self.current_job = None
            self.sheets_processor = None
    
    def _process_sheet(self, job_data: Dict) -> bool:
        """
        Process the actual Google Sheet
        
        Args:
            job_data: Job data from database
            
        Returns:
            bool: True if processing successful
        """
        job_id = job_data['id']
        sheet_id = job_data['sheet_id']
        sheet_name = job_data['sheet_name']
        case_type = job_data['case_type']
        start_row = job_data['start_row']
        num_rows = job_data['num_rows']
        
        try:
            # Create progress callback
            def progress_callback(percentage: float, message: str):
                self._update_job_progress(job_id, percentage, message)
            
            # Process the sheet range
            result = self.sheets_processor.process_sheet_range(
                sheet_id=sheet_id,
                start_row=start_row,
                num_rows=num_rows,
                progress_callback=progress_callback,
                sheet_name=sheet_name,
                processing_mode=case_type
            )
            
            # Check if processing was successful
            if result.get('success', False):
                # Update final progress
                self._update_job_progress(job_id, 100.0, "Processing completed")
                return True
            else:
                # Handle different failure scenarios
                error_msg = result.get('error_message', 'Unknown error')
                
                if result.get('status') == 'paused':
                    # Job was paused, don't mark as failed
                    self.job_manager.db.update_job_status(
                        job_id,
                        JobStatus.PAUSED,
                        error_message="Paused during processing"
                    )
                    return False
                elif result.get('status') == 'stopped':
                    # Job was stopped, mark as cancelled
                    self.job_manager.db.update_job_status(
                        job_id,
                        JobStatus.CANCELLED,
                        error_message="Stopped during processing"
                    )
                    return False
                else:
                    # Actual processing error
                    self.job_manager.db.update_job_status(
                        job_id,
                        JobStatus.FAILED,
                        error_message=error_msg
                    )
                    return False
                    
        except Exception as e:
            print(f"❌ Error in sheet processing: {e}")
            
            # Update job status
            self.job_manager.db.update_job_status(
                job_id,
                JobStatus.FAILED,
                error_message=f"Sheet processing error: {str(e)}"
            )
            
            return False
    
    def _update_job_progress(self, job_id: str, percentage: float, message: str = ""):
        """
        Update job progress in database and notify callbacks
        
        Args:
            job_id: Job identifier
            percentage: Progress percentage (0-100)
            message: Progress message
        """
        try:
            # Calculate processed rows based on percentage
            if self.current_job:
                total_rows = self.current_job['num_rows']
                processed_rows = int((percentage / 100.0) * total_rows)
                
                # Update database
                self.job_manager.db.update_job_status(
                    job_id,
                    JobStatus.RUNNING,
                    progress=percentage,
                    processed_rows=processed_rows
                )
                
                # Log progress
                self.job_manager.db.log_job_event(
                    job_id,
                    "INFO",
                    f"Progress: {percentage:.1f}% - {message}",
                    {
                        "percentage": percentage,
                        "processed_rows": processed_rows,
                        "total_rows": total_rows
                    }
                )
                
                # Notify callbacks
                self.job_manager._notify_progress(job_id, percentage, message)
                
        except Exception as e:
            print(f"❌ Error updating progress: {e}")
    
    def pause_job(self, job_id: str) -> bool:
        """
        Pause the current job (if it's the one being processed)
        
        Args:
            job_id: Job identifier
            
        Returns:
            bool: True if job was paused
        """
        if self.current_job and self.current_job['id'] == job_id:
            # This would need to be implemented in the GoogleSheetsProcessor
            # For now, we'll just return True
            print(f"⏸️ Pausing job: {job_id}")
            return True
        
        return False
    
    def stop_job(self, job_id: str) -> bool:
        """
        Stop the current job (if it's the one being processed)
        
        Args:
            job_id: Job identifier
            
        Returns:
            bool: True if job was stopped
        """
        if self.current_job and self.current_job['id'] == job_id:
            # This would need to be implemented in the GoogleSheetsProcessor
            # For now, we'll just return True
            print(f"⏹️ Stopping job: {job_id}")
            return True
        
        return False
    
    def get_current_job(self) -> Optional[Dict]:
        """Get the currently processing job"""
        return self.current_job
    
    def is_processing(self) -> bool:
        """Check if currently processing a job"""
        return self.current_job is not None
