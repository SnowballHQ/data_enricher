"""
Streamlit interface for Google Sheets processing - Persistent Authentication
"""

import streamlit as st
import json
import time
from utils.google_sheets_processor_fixed import GoogleSheetsProcessor
from utils.background_job_manager import BackgroundJobManager

def render_google_sheets_section(api_key: str):
    """Render the Google Sheets processing section in Streamlit"""
    
    st.header("🗒️ Google Sheets Real-time Processing")
    
    # Initialize processor
    if 'sheets_processor' not in st.session_state:
        st.session_state.sheets_processor = GoogleSheetsProcessor(api_key)
    
    processor = st.session_state.sheets_processor
    
    # Check authentication status
    auth_status = processor.get_auth_status()
    
    # Authentication Section
    st.subheader("🔑 Authentication Status")
    
    if auth_status['authenticated']:
        # Already authenticated
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.success("✅ **Authenticated with Google Sheets!**")
            st.info("You can now process Google Sheets without re-authenticating.")
        
        with col2:
            if st.button("🔄 Revoke & Re-authenticate", help="Clear saved authentication"):
                if processor.revoke_authentication():
                    st.success("✅ Authentication revoked!")
                    st.rerun()
                else:
                    st.error("❌ Failed to revoke authentication")
        
        # Show authentication details
        with st.expander("🔍 Authentication Details"):
            st.write(f"**Client Credentials:** {'✅ Saved' if auth_status['has_client_credentials'] else '❌ Missing'}")
            st.write(f"**Access Token:** {'✅ Valid' if auth_status['token_valid'] else '❌ Invalid'}")
            if auth_status['token_expired']:
                st.write("**Status:** Token will be auto-refreshed on next use")
        
        # Skip to processing section
        render_processing_section(processor, api_key)
        
    else:
        # Need authentication
        st.warning("⚠️ **Google Sheets authentication required**")
        
        # Check if we have saved client credentials
        if auth_status['has_client_credentials']:
            st.info("✅ **Client credentials found** - attempting to authenticate...")
            
            # Try to authenticate with saved credentials
            if processor.authenticate_oauth():
                if 'auth_url' in st.session_state:
                    # Need authorization code
                    st.markdown("**Complete authentication:**")
                    st.markdown(f"1. [**Click here to authorize**]({st.session_state.auth_url})")
                    st.markdown("2. **Copy the authorization code** and paste below:")
                    
                    auth_code = st.text_input("Authorization Code:", type="password", key="auth_code_input")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("✅ Complete Authentication", type="primary"):
                            if auth_code:
                                if processor.complete_authentication(auth_code):
                                    st.success("🎉 Authentication completed!")
                                    st.rerun()
                            else:
                                st.error("❌ Please enter the authorization code")
                    
                    with col2:
                        if st.button("🔄 Start Over"):
                            processor.revoke_authentication()
                            st.rerun()
                else:
                    st.rerun()  # Refresh to show authenticated state
            else:
                st.error("❌ Failed to start authentication with saved credentials")
                st.info("Please upload new client credentials below.")
                render_credentials_upload(processor)
        else:
            # No saved credentials - need upload
            st.info("📤 **Upload your OAuth credentials** (one-time setup)")
            render_credentials_upload(processor)

def render_credentials_upload(processor):
    """Render credentials upload section"""
    
    # Instructions
    with st.expander("📋 Setup Instructions"):
        st.markdown("""
        **One-time setup:**
        1. **Google Cloud Project** with Sheets API enabled
        2. **Desktop Application OAuth2 credentials** (client_secret.json)
           - ⚠️ Must be "Desktop Application" type, not "Web Application"
        3. Download and upload the JSON file below
        
        **After first authentication, credentials are saved locally and you won't need to re-authenticate!**
        """)
    
    # File upload
    credentials_file = st.file_uploader(
        "Upload Desktop Application OAuth2 Credentials (client_secret.json)",
        type=['json'],
        help="⚠️ Must be 'Desktop Application' type from Google Cloud Console",
        key="credentials_upload"
    )
    
    if credentials_file is not None:
        try:
            credentials_json = json.load(credentials_file)
            
            # Validate it's desktop application type
            client_type = credentials_json.get('installed', {}) if 'installed' in credentials_json else None
            if not client_type:
                st.error("❌ This appears to be a Web Application credential. Please create a Desktop Application OAuth2 credential instead.")
                st.info("💡 Go to Google Cloud Console → Create Credentials → OAuth 2.0 Client IDs → Desktop Application")
                return
            
            st.success("✅ Valid Desktop Application credentials detected!")
            
            if st.button("🚀 Start Authentication", type="primary"):
                if processor.authenticate_oauth(credentials_json):
                    st.success("✅ Credentials saved! Starting authentication...")
                    st.rerun()
                else:
                    st.error("❌ Failed to start authentication")
            
        except json.JSONDecodeError:
            st.error("❌ Invalid JSON credentials file")
        except Exception as e:
            st.error(f"❌ Error loading credentials: {e}")

def render_processing_section(processor, api_key):
    """Render the processing section when authenticated"""
    
    st.markdown("---")
    st.header("📊 Sheet Processing")
    
    # Instructions
    with st.expander("📋 Processing Instructions"):
        st.markdown("""
        **The system automatically detects your data type:**
        
        **🔹 Case A: Keywords + Description**
        - Required headers: "Company Keywords" + "Company Short Description"
        - Processing: Direct OpenAI categorization
        - Use for: Companies with existing descriptions
        
        **🔹 Case B: Company + Website**
        - Required headers: "Website" + "Company Name" (optional)
        - Processing: Website scraping → OpenAI categorization
        - Use for: Companies you want to research via their websites
        
        **📋 Sheet Format:**
        - **Row 1:** Must contain column headers
        - **Headers detected automatically** (flexible naming):
          - Keywords: "Company Keywords", "keywords", "tags"
          - Description: "Company Short Description", "description", "about"
          - Website: "Website", "URL", "web", "link"
          - Company: "Company Name", "name", "brand"
        
        **📊 Output:**
        - New columns added automatically: Category, Brand Name, Email Question, Status
        - Headers always detected from Row 1
        - Data processing starts from Row 2 (or your specified start row)
        - Enriched data added to SAME ROW in NEW COLUMNS
        - Original data is never overwritten
        """)
    
    # Sheet Configuration
    st.subheader("📊 Sheet Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        sheet_url = st.text_input(
            "Google Sheets URL",
            placeholder="https://docs.google.com/spreadsheets/d/...",
            help="Paste the full URL of your Google Sheet"
        )
    
    with col2:
        sheet_name = st.text_input(
            "Sheet Name",
            value="Sheet1",
            help="Name of the tab/sheet to process"
        )
    
    # Processing Configuration
    st.subheader("⚙️ Processing Configuration")
    
    col3, col4 = st.columns(2)
    
    with col3:
        start_row = st.number_input(
            "Start Row (Data)",
            min_value=2,
            value=2,
            help="Row number to start processing data (Row 1 is headers)"
        )
    
    with col4:
        num_rows = st.number_input(
            "Rows to Process",
            min_value=1,
            value=10,
            max_value=1000,
            help="Number of rows to process"
        )
    
    # Data Type Selection
    st.subheader("🔄 Data Type Selection")
    processing_mode = st.radio(
        "Select your data format:",
        options=["Case A: Keywords + Description", "Case B: Website + Company"],
        help="Choose based on your sheet columns",
        horizontal=True
    )
    
    # Show info based on selection
    if processing_mode == "Case A: Keywords + Description":
        st.info("📋 **Required columns:** 'Company Keywords' + 'Company Short Description'")
    else:
        st.info("🌐 **Required columns:** 'Website' + 'Company Name' (optional)")
    
    # Validation and preview
    if sheet_url:
        sheet_id = processor.extract_sheet_id(sheet_url)
        
        if sheet_id:
            st.success(f"✅ Valid sheet ID: {sheet_id[:10]}...")
            
            # Header detection and preview
            if st.button("🔍 Detect Headers & Preview", help="Analyze sheet structure and preview data"):
                with st.spinner("Analyzing sheet structure..."):
                    # Detect headers
                    header_info = processor.detect_headers(sheet_id, sheet_name)
                    
                    if header_info:
                        st.success("✅ Headers detected successfully!")
                        
                        # Show header analysis
                        col_h1, col_h2 = st.columns(2)
                        
                        with col_h1:
                            st.subheader("📋 Detected Headers")
                            for i, header in enumerate(header_info['headers']):
                                st.write(f"{chr(ord('A') + i)}: {header}")
                        
                        with col_h2:
                            st.subheader("🔍 Column Mapping")
                            mapping = header_info['column_mapping']
                            
                            if 'keywords' in mapping:
                                st.success(f"✅ Keywords: Column {mapping['keywords']}")
                            else:
                                st.error("❌ Keywords column not found")
                            
                            if 'description' in mapping:
                                st.success(f"✅ Description: Column {mapping['description']}")
                            else:
                                st.error("❌ Description column not found")
                            
                            if 'company_name' in mapping:
                                st.info(f"ℹ️ Company: Column {mapping['company_name']}")
                            else:
                                st.info("ℹ️ Company column not found (optional)")
                        
                        # Show enriched columns plan
                        existing_enriched = header_info.get('existing_enriched', False)
                        if existing_enriched:
                            st.subheader("📊 Existing Enriched Data Columns (Will be updated)")
                            st.success("✅ Found existing enriched columns - data will be updated in place")
                        else:
                            st.subheader("📊 Enriched Data Columns (Will be added)")
                            st.info("🆕 New columns will be created for enriched data")
                        
                        enriched = header_info['enriched_columns']
                        
                        col_e1, col_e2, col_e3, col_e4 = st.columns(4)
                        with col_e1:
                            icon = "🔄" if existing_enriched else "🆕"
                            st.info(f"{icon} **Category**\nColumn {enriched['category']}")
                        with col_e2:
                            icon = "🔄" if existing_enriched else "🆕"
                            st.info(f"{icon} **Brand Name**\nColumn {enriched['brand_name']}")
                        with col_e3:
                            icon = "🔄" if existing_enriched else "🆕"
                            st.info(f"{icon} **Email Question**\nColumn {enriched['email_question']}")
                        with col_e4:
                            icon = "🔄" if existing_enriched else "🆕"
                            st.info(f"{icon} **Status**\nColumn {enriched['status']}")
                        
                        # Preview data
                        preview_df = processor.get_sheet_data(
                            sheet_id, start_row, min(5, num_rows), 
                            header_info['column_mapping'], sheet_name
                        )
                        
                        if preview_df is not None:
                            st.subheader("👀 Data Preview")
                            st.dataframe(preview_df, width='stretch')
                            
                            # Show processing readiness based on selected mode
                            missing_cols = []
                            if processing_mode == "Case A: Keywords + Description":
                                if 'keywords' not in mapping:
                                    missing_cols.append('Company Keywords')
                                if 'description' not in mapping:
                                    missing_cols.append('Company Short Description')
                            elif processing_mode == "Case B: Website + Company":
                                if 'website' not in mapping:
                                    missing_cols.append('Website')
                                # company_name is optional for Case B
                            
                            if missing_cols:
                                st.error(f"❌ Cannot process: Missing required columns: {', '.join(missing_cols)}")
                            else:
                                st.success("✅ Ready to process! All required columns detected.")
                                
                                # Store header info for processing
                                st.session_state.header_info = header_info
                    else:
                        st.error("❌ Failed to detect headers")
            
            # Main processing button
            if st.session_state.get('header_info'):
                st.markdown("---")
                
                # Processing summary
                header_info = st.session_state.header_info
                mapping = header_info['column_mapping']
                
                st.subheader("🚀 Ready to Process")
                existing_enriched = header_info.get('existing_enriched', False)
                action = "Update" if existing_enriched else "Add"
                st.info(f"""
                **Processing Plan:**
                - Start from Row {start_row} (data rows)
                - Process {num_rows} rows
                - Headers from Row 1
                - {action} enriched data in columns {header_info['enriched_columns']['category']}-{header_info['enriched_columns']['status']}
                - {'🔄 Existing columns will be updated' if existing_enriched else '🆕 New columns will be created'}
                """)
                
                # Processing buttons
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("🚀 Start Processing", type="primary", width='stretch'):
                        # Validate again before processing based on selected mode
                        missing_cols = []
                        if processing_mode == "Case A: Keywords + Description":
                            if 'keywords' not in mapping:
                                missing_cols.append('Company Keywords')
                            if 'description' not in mapping:
                                missing_cols.append('Company Short Description')
                        elif processing_mode == "Case B: Website + Company":
                            if 'website' not in mapping:
                                missing_cols.append('Website')
                            # company_name is optional for Case B
                        
                        if missing_cols:
                            st.error(f"❌ Cannot process: Missing required columns: {', '.join(missing_cols)}")
                        else:
                            # Convert processing_mode to the format expected by processor
                            case_type = "CASE_A" if processing_mode == "Case A: Keywords + Description" else "CASE_B"
                            process_google_sheet(processor, sheet_id, start_row, num_rows, sheet_name, case_type)
                
                with col2:
                    if st.button("🔄 Queue for Background Processing", width='stretch'):
                        # Validate columns for background processing
                        missing_cols = []
                        if processing_mode == "Case A: Keywords + Description":
                            if 'keywords' not in mapping:
                                missing_cols.append('Company Keywords')
                            if 'description' not in mapping:
                                missing_cols.append('Company Short Description')
                        elif processing_mode == "Case B: Website + Company":
                            if 'website' not in mapping:
                                missing_cols.append('Website')
                        
                        if missing_cols:
                            st.error(f"❌ Cannot queue: Missing required columns: {', '.join(missing_cols)}")
                        else:
                            # Queue for background processing
                            queue_for_background_processing(api_key, sheet_id, sheet_name, processing_mode, start_row, num_rows)
            else:
                st.info("👆 Please detect headers first to enable processing")
                
        else:
            st.error("❌ Invalid Google Sheets URL")
    else:
        st.info("👆 Please enter a Google Sheets URL to continue")

def process_google_sheet(processor, sheet_id, start_row, num_rows, sheet_name, case_type):
    """Process Google Sheet with real-time updates"""
    
    # Initialize progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    results_container = st.empty()
    
    # Control buttons container
    controls_container = st.empty()
    
    # Initialize session state for controls
    if 'processing_paused' not in st.session_state:
        st.session_state.processing_paused = False
    if 'processing_stopped' not in st.session_state:
        st.session_state.processing_stopped = False
    
    # Reset control states at start
    st.session_state.processing_paused = False
    st.session_state.processing_stopped = False
    
    # Start time tracking
    start_time = time.time()
    
    # Progress callback function with controls
    def update_progress(percentage, message):
        progress_bar.progress(percentage / 100)
        status_text.text(message)
        
        # Show control buttons during processing
        with controls_container.container():
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("⏸️ Pause", key=f"pause_{time.time()}"):
                    st.session_state.processing_paused = True
                    st.warning("⏸️ Pause requested...")
            
            with col2:
                if st.button("⏹️ Stop", key=f"stop_{time.time()}"):
                    st.session_state.processing_stopped = True
                    st.error("⏹️ Stop requested...")
            
            with col3:
                if st.session_state.processing_paused:
                    if st.button("▶️ Resume", key=f"resume_{time.time()}"):
                        st.session_state.processing_paused = False
                        st.success("▶️ Resuming processing...")
        
        # Small delay to allow UI updates
        time.sleep(0.1)
    
    try:
        # Start processing
        with st.spinner("🔄 Initializing processing..."):
            results = processor.process_sheet_range(
                sheet_id=sheet_id,
                start_row=start_row,
                num_rows=num_rows,
                progress_callback=update_progress,
                sheet_name=sheet_name,
                processing_mode=case_type
            )
        
        # Display results
        if results.get("success"):
            # Handle different completion states
            status = results.get("status", "completed")
            
            if status == "paused":
                progress_bar.progress(results["processed_count"] / num_rows)
                status_text.text("⏸️ Processing paused!")
                st.warning("⏸️ Processing has been paused. You can resume or start a new processing run.")
            elif status == "stopped":
                progress_bar.progress(results["processed_count"] / num_rows)
                status_text.text("⏹️ Processing stopped!")
                st.error("⏹️ Processing has been stopped by user request.")
            else:
                # Complete success
                progress_bar.progress(1.0)
                status_text.text("✅ Processing completed!")
                st.success("🎉 Processing completed successfully!")
            
            total_time = results["total_time"]
            
            with results_container.container():
                
                # Metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("✅ Processed", results["processed_count"])
                with col2:
                    st.metric("🎯 Successful", results["success_count"])
                with col3:
                    st.metric("❌ Errors", results["error_count"])
                with col4:
                    st.metric("⏱️ Total Time", f"{total_time:.1f}s")
                
                # Additional stats
                success_rate = (results["success_count"] / results["processed_count"]) * 100 if results["processed_count"] > 0 else 0
                avg_time = results["avg_time_per_row"]
                
                col5, col6 = st.columns(2)
                with col5:
                    st.metric("📊 Success Rate", f"{success_rate:.1f}%")
                with col6:
                    st.metric("⚡ Avg Time/Row", f"{avg_time:.2f}s")
                
                # Column information
                if "column_mapping" in results and "enriched_columns" in results:
                    with st.expander("📊 Column Information"):
                        col_info1, col_info2 = st.columns(2)
                        
                        with col_info1:
                            st.write("**Input Columns Used:**")
                            for key, col in results["column_mapping"].items():
                                st.write(f"• {key.title()}: Column {col}")
                        
                        with col_info2:
                            st.write("**Enriched Data Added:**")
                            for key, col in results["enriched_columns"].items():
                                st.write(f"• {key.replace('_', ' ').title()}: Column {col}")
                
                # Skipped rows info
                if results["skipped_rows"]:
                    with st.expander(f"⚠️ Skipped Rows ({len(results['skipped_rows'])})"):
                        st.write("Row numbers with errors or empty data:")
                        st.write(", ".join(map(str, results["skipped_rows"])))
                
                st.success("🔗 **Check your Google Sheet to see the real-time results in the new columns!**")
        
        else:
            st.error(f"❌ Processing failed: {results.get('error', 'Unknown error')}")
    
    except Exception as e:
        st.error(f"❌ Processing error: {str(e)}")
    
    finally:
        # Clean up progress indicators and controls
        controls_container.empty()
        progress_bar.empty()
        status_text.empty()

def queue_for_background_processing(api_key: str, sheet_id: str, sheet_name: str, 
                                  processing_mode: str, start_row: int, num_rows: int):
    """Queue a job for background processing"""
    
    try:
        # Initialize background job manager
        if 'background_job_manager' not in st.session_state:
            st.session_state.background_job_manager = BackgroundJobManager()
        
        job_manager = st.session_state.background_job_manager
        
        # Convert processing mode to case type
        case_type = "CASE_A" if processing_mode == "Case A: Keywords + Description" else "CASE_B"
        
        # Create background job
        job_id = job_manager.create_job(
            sheet_id=sheet_id,
            sheet_name=sheet_name,
            case_type=case_type,
            start_row=start_row,
            num_rows=num_rows,
            api_key=api_key,
            metadata={
                'created_from_sheets_tab': True,
                'processing_mode': processing_mode
            }
        )
        
        # Show success message
        st.success("✅ Job queued for background processing!")
        st.info(f"**Job ID:** `{job_id}`")
        st.info(f"**Status:** Queued - will start processing automatically")
        
        # Show next steps
        st.markdown("""
        **Next Steps:**
        1. Go to the **Background Processing** tab to monitor progress
        2. The job will start processing automatically when a worker is available
        3. You can continue working on other sheets while this processes in the background
        """)
        
        # Show queue information
        queue_info = job_manager.get_queue_info()
        if queue_info.queue_size > 1:
            st.info(f"📊 **Queue Status:** {queue_info.queue_size} jobs in queue, estimated wait time: {queue_info.estimated_wait_time:.1f} minutes")
        
    except Exception as e:
        st.error(f"❌ Error queuing job for background processing: {str(e)}")
        st.error("Please try again or use the regular processing option")
