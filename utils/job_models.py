"""
Job Models and Data Structures for Background Processing
"""

from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum

class JobStatus(Enum):
    """Job status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class CaseType(Enum):
    """Processing case type enumeration"""
    CASE_A = "CASE_A"  # Keywords + Description
    CASE_B = "CASE_B"  # Website + Company

class LogLevel(Enum):
    """Log level enumeration"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

@dataclass
class JobData:
    """Job data structure"""
    id: str
    sheet_id: str
    sheet_name: str
    case_type: CaseType
    start_row: int
    num_rows: int
    status: JobStatus = JobStatus.PENDING
    progress: float = 0.0
    processed_rows: int = 0
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    api_key_hash: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.created_at is None:
            self.created_at = datetime.now()
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for database storage"""
        data = asdict(self)
        # Convert enums to strings
        data['status'] = self.status.value
        data['case_type'] = self.case_type.value
        # Convert datetime objects to ISO strings
        for field in ['created_at', 'started_at', 'completed_at']:
            if data[field]:
                data[field] = data[field].isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'JobData':
        """Create JobData from dictionary"""
        # Convert string enums back to enum objects
        if isinstance(data.get('status'), str):
            data['status'] = JobStatus(data['status'])
        if isinstance(data.get('case_type'), str):
            data['case_type'] = CaseType(data['case_type'])
        
        # Convert ISO strings back to datetime objects
        for field in ['created_at', 'started_at', 'completed_at']:
            if data.get(field) and isinstance(data[field], str):
                data[field] = datetime.fromisoformat(data[field])
        
        return cls(**data)

@dataclass
class JobLog:
    """Job log entry"""
    id: Optional[int] = None
    job_id: str = ""
    timestamp: Optional[datetime] = None
    level: LogLevel = LogLevel.INFO
    message: str = ""
    details: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.details is None:
            self.details = {}
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for database storage"""
        data = asdict(self)
        data['level'] = self.level.value
        if self.timestamp:
            data['timestamp'] = self.timestamp.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'JobLog':
        """Create JobLog from dictionary"""
        if isinstance(data.get('level'), str):
            data['level'] = LogLevel(data['level'])
        if data.get('timestamp') and isinstance(data['timestamp'], str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)

@dataclass
class JobProgress:
    """Job progress tracking"""
    job_id: str
    current_row: int
    total_rows: int
    processed_rows: int
    percentage: float
    estimated_completion: Optional[datetime] = None
    rows_per_minute: float = 0.0
    errors_count: int = 0
    skipped_rows: int = 0
    
    def update_progress(self, processed_rows: int, current_row: int = None):
        """Update progress metrics"""
        self.processed_rows = processed_rows
        if current_row is not None:
            self.current_row = current_row
        
        if self.total_rows > 0:
            self.percentage = (self.processed_rows / self.total_rows) * 100
        
        # Calculate processing rate
        if self.processed_rows > 0:
            # This would need to be calculated based on time elapsed
            # For now, we'll set it to 0 and let the caller calculate
            pass

@dataclass
class JobStats:
    """Job statistics and metrics"""
    total_jobs: int = 0
    pending_jobs: int = 0
    running_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    cancelled_jobs: int = 0
    total_processed_rows: int = 0
    average_processing_time: float = 0.0
    success_rate: float = 0.0
    
    def calculate_success_rate(self) -> float:
        """Calculate job success rate"""
        if self.total_jobs == 0:
            return 0.0
        
        successful = self.completed_jobs
        total_attempted = self.completed_jobs + self.failed_jobs + self.cancelled_jobs
        
        if total_attempted == 0:
            return 0.0
        
        return (successful / total_attempted) * 100

@dataclass
class WorkerInfo:
    """Background worker information"""
    worker_id: str
    status: str  # "idle", "busy", "error"
    current_job_id: Optional[str] = None
    started_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    jobs_processed: int = 0
    errors_count: int = 0
    
    def is_alive(self, timeout_seconds: int = 30) -> bool:
        """Check if worker is alive based on last heartbeat"""
        if not self.last_heartbeat:
            return False
        
        time_since_heartbeat = (datetime.now() - self.last_heartbeat).total_seconds()
        return time_since_heartbeat < timeout_seconds

@dataclass
class JobQueueInfo:
    """Job queue information"""
    queue_size: int
    estimated_wait_time: float  # in minutes
    active_workers: int
    max_workers: int
    queue_position: Optional[int] = None  # For specific job

class JobError(Exception):
    """Custom exception for job-related errors"""
    def __init__(self, job_id: str, message: str, details: Dict = None):
        self.job_id = job_id
        self.message = message
        self.details = details or {}
        super().__init__(f"Job {job_id}: {message}")

class JobValidationError(JobError):
    """Exception for job validation errors"""
    pass

class JobProcessingError(JobError):
    """Exception for job processing errors"""
    pass

class JobNotFoundError(JobError):
    """Exception for when job is not found"""
    def __init__(self, job_id: str):
        super().__init__(job_id, f"Job {job_id} not found")

# Utility functions
def create_job_data(sheet_id: str, sheet_name: str, case_type: CaseType, 
                   start_row: int, num_rows: int, api_key: str = None,
                   metadata: Dict = None) -> JobData:
    """Create a new JobData instance with required fields"""
    import uuid
    import hashlib
    
    job_id = str(uuid.uuid4())
    api_key_hash = None
    if api_key:
        api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:16]
    
    return JobData(
        id=job_id,
        sheet_id=sheet_id,
        sheet_name=sheet_name,
        case_type=case_type,
        start_row=start_row,
        num_rows=num_rows,
        api_key_hash=api_key_hash,
        metadata=metadata or {}
    )

def validate_job_data(job_data: JobData) -> List[str]:
    """Validate job data and return list of errors"""
    errors = []
    
    if not job_data.sheet_id or not job_data.sheet_id.strip():
        errors.append("Sheet ID is required")
    
    if not job_data.sheet_name or not job_data.sheet_name.strip():
        errors.append("Sheet name is required")
    
    if job_data.start_row < 1:
        errors.append("Start row must be greater than 0")
    
    if job_data.num_rows < 1:
        errors.append("Number of rows must be greater than 0")
    
    if job_data.case_type not in [CaseType.CASE_A, CaseType.CASE_B]:
        errors.append("Case type must be CASE_A or CASE_B")
    
    return errors
