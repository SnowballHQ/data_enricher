"""
Background Processing UI for Streamlit
User interface for managing background jobs and monitoring progress
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import plotly.express as px
import plotly.graph_objects as go

from .background_job_manager import BackgroundJobManager
from .job_models import JobStatus, CaseType, JobStats

def render_background_processing_section(api_key: str):
    """Render the background processing section in Streamlit"""
    
    st.header("🔄 Background Processing")
    st.markdown("Process multiple Google Sheets concurrently with real-time monitoring")
    
    # Initialize job manager in session state
    if 'background_job_manager' not in st.session_state:
        st.session_state.background_job_manager = BackgroundJobManager()
    
    job_manager = st.session_state.background_job_manager
    
    # Main tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Dashboard", 
        "➕ Create Job", 
        "📋 Job List", 
        "⚙️ Settings"
    ])
    
    with tab1:
        render_dashboard(job_manager)
    
    with tab2:
        render_create_job(job_manager, api_key)
    
    with tab3:
        render_job_list(job_manager)
    
    with tab4:
        render_settings(job_manager)

def render_dashboard(job_manager: BackgroundJobManager):
    """Render the main dashboard with statistics and monitoring"""
    
    st.subheader("📊 Processing Dashboard")
    
    # Auto-refresh every 5 seconds
    if st.checkbox("🔄 Auto-refresh (5s)", value=True):
        time.sleep(0.1)  # Small delay to allow UI updates
        st.rerun()
    
    # Get current statistics
    stats = job_manager.get_statistics()
    queue_info = job_manager.get_queue_info()
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Jobs", 
            stats.total_jobs,
            delta=None
        )
    
    with col2:
        st.metric(
            "Running Jobs", 
            stats.running_jobs,
            delta=None
        )
    
    with col3:
        st.metric(
            "Queue Size", 
            queue_info.queue_size,
            delta=None
        )
    
    with col4:
        st.metric(
            "Success Rate", 
            f"{stats.success_rate:.1f}%",
            delta=None
        )
    
    st.markdown("---")
    
    # Status distribution chart
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Job Status Distribution")
        
        status_data = {
            'Status': ['Pending', 'Running', 'Completed', 'Failed', 'Cancelled'],
            'Count': [
                stats.pending_jobs,
                stats.running_jobs,
                stats.completed_jobs,
                stats.failed_jobs,
                stats.cancelled_jobs
            ]
        }
        
        df_status = pd.DataFrame(status_data)
        
        # Create pie chart
        fig = px.pie(
            df_status, 
            values='Count', 
            names='Status',
            color_discrete_map={
                'Pending': '#FFA500',
                'Running': '#1f77b4',
                'Completed': '#2ca02c',
                'Failed': '#d62728',
                'Cancelled': '#9467bd'
            }
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, width='stretch')
    
    with col2:
        st.subheader("⏱️ Queue Information")
        
        queue_data = {
            'Metric': ['Queue Size', 'Active Workers', 'Max Workers', 'Est. Wait Time'],
            'Value': [
                f"{queue_info.queue_size} jobs",
                f"{queue_info.active_workers} workers",
                f"{queue_info.max_workers} workers",
                f"{queue_info.estimated_wait_time:.1f} min"
            ]
        }
        
        df_queue = pd.DataFrame(queue_data)
        st.dataframe(df_queue, width='stretch', hide_index=True)
    
    # Recent activity
    st.subheader("📋 Recent Activity")
    
    recent_jobs = job_manager.get_all_jobs(limit=10)
    
    if recent_jobs:
        # Convert to DataFrame for display
        df_jobs = pd.DataFrame(recent_jobs)
        
        # Format timestamps
        if 'created_at' in df_jobs.columns:
            df_jobs['created_at'] = pd.to_datetime(df_jobs['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Select columns to display
        display_cols = ['id', 'sheet_name', 'case_type', 'status', 'progress', 'created_at']
        available_cols = [col for col in display_cols if col in df_jobs.columns]
        
        if available_cols:
            st.dataframe(
                df_jobs[available_cols].rename(columns={
                    'id': 'Job ID',
                    'sheet_name': 'Sheet Name',
                    'case_type': 'Type',
                    'status': 'Status',
                    'progress': 'Progress %',
                    'created_at': 'Created'
                }),
                width='stretch'
            )
        else:
            st.info("No recent jobs found")
    else:
        st.info("No jobs found. Create your first job to get started!")

def render_create_job(job_manager: BackgroundJobManager, api_key: str):
    """Render the job creation interface"""
    
    st.subheader("➕ Create Background Job")
    
    # Instructions
    with st.expander("📋 Instructions"):
        st.markdown("""
        **Create a background job to process Google Sheets:**
        
        1. **Enter Google Sheets URL** - Paste the full URL of your sheet
        2. **Select Processing Type** - Choose Case A (keywords+description) or Case B (website+company)
        3. **Configure Processing Range** - Set start row and number of rows to process
        4. **Submit Job** - The job will be queued and processed in the background
        
        **Benefits of Background Processing:**
        - Process multiple sheets simultaneously
        - Continue working while sheets process
        - Real-time progress monitoring
        - Job persistence across app restarts
        """)
    
    # Job creation form
    with st.form("create_job_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            sheet_url = st.text_input(
                "Google Sheets URL",
                placeholder="https://docs.google.com/spreadsheets/d/...",
                help="Paste the full URL of your Google Sheet"
            )
            
            case_type = st.selectbox(
                "Processing Type",
                options=["CASE_A", "CASE_B"],
                format_func=lambda x: {
                    "CASE_A": "Case A: Keywords + Description",
                    "CASE_B": "Case B: Website + Company"
                }[x],
                help="Select based on your data structure"
            )
        
        with col2:
            start_row = st.number_input(
                "Start Row",
                min_value=2,
                value=2,
                help="Row number to start processing from (usually 2 for data after headers)"
            )
            
            num_rows = st.number_input(
                "Number of Rows",
                min_value=1,
                value=100,
                help="Total number of rows to process"
            )
        
        # Additional options
        st.subheader("⚙️ Advanced Options")
        
        col1, col2 = st.columns(2)
        
        with col1:
            sheet_name = st.text_input(
                "Sheet Name (Optional)",
                value="Sheet1",
                help="Name of the specific sheet to process"
            )
        
        with col2:
            priority = st.selectbox(
                "Priority",
                options=["normal", "high"],
                help="Job priority in the queue"
            )
        
        # Check for duplicate jobs
        duplicate_job = None
        if sheet_url:
            try:
                sheet_id = extract_sheet_id(sheet_url)
                if sheet_id:
                    # Check for existing jobs with same parameters
                    existing_jobs = job_manager.get_all_jobs(limit=100)
                    for job in existing_jobs:
                        if (job['sheet_id'] == sheet_id and 
                            job['case_type'] == case_type and
                            job['start_row'] == start_row and
                            job['num_rows'] == num_rows and
                            job['sheet_name'] == sheet_name and
                            job['status'] in ['pending', 'running', 'paused']):
                            duplicate_job = job
                            break
            except Exception:
                pass
        
        # Show duplicate warning
        if duplicate_job:
            st.warning(f"⚠️ **Duplicate Job Detected!**")
            st.info(f"""
            A job with identical parameters already exists:
            - **Job ID:** `{duplicate_job['id'][:8]}...`
            - **Status:** {duplicate_job['status'].title()}
            - **Progress:** {duplicate_job.get('progress', 0):.1f}%
            - **Created:** {duplicate_job.get('created_at', 'Unknown')}
            """)
        
        # Submit button (disabled if duplicate)
        submit_disabled = duplicate_job is not None
        submitted = st.form_submit_button(
            "🚀 Create Background Job" if not submit_disabled else "⚠️ Duplicate Job Exists",
            type="primary",
            disabled=submit_disabled
        )
        
        if submitted:
            if not sheet_url:
                st.error("❌ Please enter a Google Sheets URL")
            elif not sheet_url.startswith("https://docs.google.com/spreadsheets/"):
                st.error("❌ Please enter a valid Google Sheets URL")
            else:
                try:
                    # Extract sheet ID from URL
                    sheet_id = extract_sheet_id(sheet_url)
                    if not sheet_id:
                        st.error("❌ Could not extract sheet ID from URL")
                    else:
                        # Create job
                        job_id = job_manager.create_job(
                            sheet_id=sheet_id,
                            sheet_name=sheet_name,
                            case_type=case_type,
                            start_row=start_row,
                            num_rows=num_rows,
                            api_key=api_key,
                            metadata={
                                'priority': priority,
                                'created_by_ui': True,
                                'sheet_url': sheet_url
                            }
                        )
                        
                        st.success(f"✅ Job created successfully!")
                        st.info(f"**Job ID:** `{job_id}`")
                        st.info(f"**Status:** Queued for processing")
                        
                        # Show next steps
                        st.markdown("""
                        **Next Steps:**
                        1. Go to the **Dashboard** tab to monitor progress
                        2. Go to the **Job List** tab to manage your jobs
                        3. The job will start processing automatically
                        """)
                        
                except Exception as e:
                    st.error(f"❌ Error creating job: {str(e)}")

def render_job_list(job_manager: BackgroundJobManager):
    """Render the job list with management controls"""
    
    st.subheader("📋 Job Management")
    
    # Auto-refresh for running jobs
    if st.checkbox("🔄 Auto-refresh (3s)", value=True, help="Automatically refresh to show latest job status"):
        time.sleep(0.1)
        st.rerun()
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        status_filter = st.selectbox(
            "Filter by Status",
            options=["All", "pending", "running", "completed", "failed", "cancelled"],
            help="Filter jobs by status"
        )
    
    with col2:
        case_filter = st.selectbox(
            "Filter by Type",
            options=["All", "CASE_A", "CASE_B"],
            help="Filter jobs by processing type"
        )
    
    with col3:
        limit = st.selectbox(
            "Show Jobs",
            options=[10, 25, 50, 100],
            index=1,
            help="Number of jobs to display"
        )
    
    # Get jobs based on filters
    if status_filter == "All":
        jobs = job_manager.get_all_jobs(limit=limit)
    else:
        jobs = job_manager.get_jobs_by_status(status_filter)
    
    # Debug information
    st.write(f"🔍 Debug: Found {len(jobs)} jobs before case filter")
    
    # Apply case type filter
    if case_filter != "All":
        jobs = [job for job in jobs if job.get('case_type') == case_filter]
    
    st.write(f"🔍 Debug: Found {len(jobs)} jobs after case filter")
    
    if not jobs:
        st.info("No jobs found matching the selected filters")
        st.write("🔍 Debug: Available jobs in database:")
        all_jobs = job_manager.get_all_jobs(limit=5)
        for job in all_jobs:
            st.write(f"- {job['id'][:8]}... - {job['sheet_name']} - {job['status']} - {job['case_type']}")
        return
    
    # Display jobs
    st.markdown(f"**Found {len(jobs)} jobs**")
    
    # Show running jobs prominently
    running_jobs = [job for job in jobs if job['status'] == 'running']
    if running_jobs:
        st.subheader("🔄 Currently Running Jobs")
        for job in running_jobs:
            with st.container():
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    st.write(f"**{job['sheet_name']}** - {job['case_type']}")
                    st.write(f"Rows: {job['start_row']} to {job['start_row'] + job['num_rows'] - 1}")
                
                with col2:
                    progress = job.get('progress', 0) / 100.0
                    st.progress(progress)
                    st.write(f"{job.get('progress', 0):.1f}% complete")
                
                with col3:
                    if st.button("❌ Cancel", key=f"cancel_running_{job['id']}"):
                        if job_manager.cancel_job(job['id']):
                            st.success("Job cancelled")
                            st.rerun()
                        else:
                            st.error("Failed to cancel job")
        
        st.markdown("---")
    
    # Show all jobs in expandable sections
    for job in jobs:
        status_emoji = {
            'pending': '⏳',
            'running': '🔄',
            'completed': '✅',
            'failed': '❌',
            'cancelled': '🚫',
            'paused': '⏸️'
        }.get(job['status'], '❓')
        
        with st.expander(f"{status_emoji} Job {job['id'][:8]}... - {job['sheet_name']} ({job['status'].title()})"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Sheet ID:** `{job['sheet_id']}`")
                st.write(f"**Case Type:** {job['case_type']}")
                st.write(f"**Rows:** {job['start_row']} to {job['start_row'] + job['num_rows'] - 1}")
                st.write(f"**Created:** {job.get('created_at', 'Unknown')}")
            
            with col2:
                st.write(f"**Status:** {job['status'].title()}")
                st.write(f"**Progress:** {job.get('progress', 0):.1f}%")
                st.write(f"**Processed:** {job.get('processed_rows', 0)} rows")
                
                if job.get('error_message'):
                    st.error(f"**Error:** {job['error_message']}")
            
            # Progress bar for running/completed jobs
            if job['status'] in ['running', 'completed']:
                progress = job.get('progress', 0) / 100.0
                st.progress(progress)
            
            # Job controls
            st.markdown("**Controls:**")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if job['status'] == 'pending':
                    if st.button("⏸️ Pause", key=f"pause_{job['id']}"):
                        if job_manager.pause_job(job['id']):
                            st.success("Job paused")
                            st.rerun()
                        else:
                            st.error("Failed to pause job")
                
                elif job['status'] == 'paused':
                    if st.button("▶️ Resume", key=f"resume_{job['id']}"):
                        if job_manager.resume_job(job['id']):
                            st.success("Job resumed")
                            st.rerun()
                        else:
                            st.error("Failed to resume job")
            
            with col2:
                if job['status'] in ['pending', 'running', 'paused']:
                    if st.button("❌ Cancel", key=f"cancel_{job['id']}"):
                        if job_manager.cancel_job(job['id']):
                            st.success("Job cancelled")
                            st.rerun()
                        else:
                            st.error("Failed to cancel job")
            
            with col3:
                if st.button("🗑️ Delete", key=f"delete_{job['id']}"):
                    if job_manager.delete_job(job['id']):
                        st.success("Job deleted")
                        st.rerun()
                    else:
                        st.error("Failed to delete job")
            
            with col4:
                if st.button("📊 Details", key=f"details_{job['id']}"):
                    show_job_details(job_manager, job['id'])

def render_settings(job_manager: BackgroundJobManager):
    """Render settings and configuration"""
    
    st.subheader("⚙️ Background Processing Settings")
    
    # Current configuration
    st.markdown("**Current Configuration:**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.info(f"**Max Workers:** {job_manager.max_workers}")
        st.info(f"**Database:** background_jobs.db")
    
    with col2:
        queue_info = job_manager.get_queue_info()
        st.info(f"**Active Workers:** {queue_info.active_workers}")
        st.info(f"**Queue Size:** {queue_info.queue_size}")
    
    st.markdown("---")
    
    # Database statistics
    st.subheader("📊 Database Statistics")
    
    try:
        db_stats = job_manager.db.get_database_stats()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Jobs", db_stats['total_jobs'])
        
        with col2:
            st.metric("Database Size", f"{db_stats['database_size_mb']} MB")
        
        with col3:
            success_rate = job_manager.get_statistics().success_rate
            st.metric("Success Rate", f"{success_rate:.1f}%")
        
        # Status breakdown
        st.markdown("**Jobs by Status:**")
        status_df = pd.DataFrame([
            {"Status": status, "Count": count}
            for status, count in db_stats['status_counts'].items()
        ])
        st.dataframe(status_df, width='stretch', hide_index=True)
        
    except Exception as e:
        st.error(f"Error loading database statistics: {e}")
    
    st.markdown("---")
    
    # Maintenance actions
    st.subheader("🔧 Maintenance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🧹 Clean Old Jobs", help="Remove completed jobs older than 30 days"):
            try:
                cleaned = job_manager.db.cleanup_old_jobs(days_old=30)
                st.success(f"Cleaned up {cleaned} old jobs")
            except Exception as e:
                st.error(f"Error cleaning jobs: {e}")
    
    with col2:
        if st.button("🔄 Refresh Statistics", help="Update all statistics"):
            st.rerun()

def show_job_details(job_manager: BackgroundJobManager, job_id: str):
    """Show detailed information about a specific job"""
    
    job = job_manager.get_job_status(job_id)
    if not job:
        st.error("Job not found")
        return
    
    st.subheader(f"Job Details: {job_id[:8]}...")
    
    # Basic information
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"**Sheet ID:** `{job['sheet_id']}`")
        st.write(f"**Sheet Name:** {job['sheet_name']}")
        st.write(f"**Case Type:** {job['case_type']}")
        st.write(f"**Start Row:** {job['start_row']}")
        st.write(f"**Total Rows:** {job['num_rows']}")
    
    with col2:
        st.write(f"**Status:** {job['status']}")
        st.write(f"**Progress:** {job.get('progress', 0):.1f}%")
        st.write(f"**Processed Rows:** {job.get('processed_rows', 0)}")
        st.write(f"**Created:** {job.get('created_at', 'Unknown')}")
        st.write(f"**Started:** {job.get('started_at', 'Not started')}")
        st.write(f"**Completed:** {job.get('completed_at', 'Not completed')}")
    
    # Progress bar
    if job['status'] in ['running', 'completed']:
        progress = job.get('progress', 0) / 100.0
        st.progress(progress)
    
    # Error information
    if job.get('error_message'):
        st.error(f"**Error:** {job['error_message']}")
    
    # Job logs
    st.subheader("📋 Job Logs")
    
    try:
        logs = job_manager.db.get_job_logs(job_id, limit=20)
        
        if logs:
            log_df = pd.DataFrame(logs)
            log_df['timestamp'] = pd.to_datetime(log_df['timestamp']).dt.strftime('%H:%M:%S')
            
            st.dataframe(
                log_df[['timestamp', 'level', 'message']].rename(columns={
                    'timestamp': 'Time',
                    'level': 'Level',
                    'message': 'Message'
                }),
                width='stretch',
                hide_index=True
            )
        else:
            st.info("No logs available for this job")
    
    except Exception as e:
        st.error(f"Error loading job logs: {e}")

def extract_sheet_id(sheet_url: str) -> Optional[str]:
    """Extract Google Sheets ID from URL"""
    try:
        # Handle different URL formats
        if '/d/' in sheet_url:
            # Format: https://docs.google.com/spreadsheets/d/SHEET_ID/edit
            parts = sheet_url.split('/d/')
            if len(parts) > 1:
                sheet_id = parts[1].split('/')[0]
                return sheet_id
        
        return None
    except Exception:
        return None
