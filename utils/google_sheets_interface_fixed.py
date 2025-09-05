"""
Streamlit interface for Google Sheets processing - Fixed version
"""

import streamlit as st
import json
import time
from utils.google_sheets_processor_fixed import GoogleSheetsProcessor

def render_google_sheets_section(api_key: str):
    """Render the Google Sheets processing section in Streamlit"""
    
    st.header("🗒️ Google Sheets Real-time Processing")
    
    # Instructions
    with st.expander("📋 Setup Instructions & Requirements"):
        st.markdown("""
        **Prerequisites:**
        1. **Google Cloud Project** with Sheets API enabled
        2. **Desktop Application OAuth2 credentials** (client_secret.json)
           - ⚠️ Must be "Desktop Application" type, not "Web Application"
        3. **Google Sheet** with edit permissions
        
        **Sheet Format Requirements:**
        - **Row 1:** Must contain column headers
        - **Headers detected automatically** (any of these names work):
          - Keywords: "Company Keywords", "keywords", "tags"
          - Description: "Company Short Description", "description", "about"
          - Company: "Company Name", "name", "brand"
        
        **New columns will be added automatically:**
        - Category, Brand Name, Email Question, Status
        
        **Processing Logic:**
        - Headers always detected from Row 1
        - Data processing starts from Row 2 (or your specified start row)
        - Enriched data added to SAME ROW in NEW COLUMNS
        - Original data is never overwritten
        """)
    
    # OAuth2 Credentials Upload
    st.subheader("🔑 Authentication")
    
    credentials_file = st.file_uploader(
        "Upload Desktop Application OAuth2 Credentials (client_secret.json)",
        type=['json'],
        help="⚠️ Must be 'Desktop Application' type from Google Cloud Console"
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
            
            # Initialize processor
            if 'sheets_processor' not in st.session_state:
                st.session_state.sheets_processor = GoogleSheetsProcessor(api_key)
            
            processor = st.session_state.sheets_processor
            
            # Authenticate
            if processor.authenticate_oauth(credentials_json):
                st.success("✅ Authenticated with Google Sheets!")
                
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
                                    st.subheader("📊 Enriched Data Columns (Will be added)")
                                    enriched = header_info['enriched_columns']
                                    
                                    col_e1, col_e2, col_e3, col_e4 = st.columns(4)
                                    with col_e1:
                                        st.info(f"**Category**\nColumn {enriched['category']}")
                                    with col_e2:
                                        st.info(f"**Brand Name**\nColumn {enriched['brand_name']}")
                                    with col_e3:
                                        st.info(f"**Email Question**\nColumn {enriched['email_question']}")
                                    with col_e4:
                                        st.info(f"**Status**\nColumn {enriched['status']}")
                                    
                                    # Preview data
                                    preview_df = processor.get_sheet_data(
                                        sheet_id, start_row, min(5, num_rows), 
                                        header_info['column_mapping'], sheet_name
                                    )
                                    
                                    if preview_df is not None:
                                        st.subheader("👀 Data Preview")
                                        st.dataframe(preview_df, use_container_width=True)
                                        
                                        # Show processing readiness
                                        missing_cols = []
                                        if 'keywords' not in mapping:
                                            missing_cols.append('keywords')
                                        if 'description' not in mapping:
                                            missing_cols.append('description')
                                        
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
                            st.info(f"""
                            **Processing Plan:**
                            - Start from Row {start_row} (data rows)
                            - Process {num_rows} rows
                            - Headers from Row 1
                            - Add enriched data in columns {header_info['enriched_columns']['category']}-{header_info['enriched_columns']['status']}
                            """)
                            
                            if st.button("🚀 Start Processing", type="primary", use_container_width=True):
                                # Validate again before processing
                                missing_cols = []
                                if 'keywords' not in mapping:
                                    missing_cols.append('keywords')
                                if 'description' not in mapping:
                                    missing_cols.append('description')
                                
                                if missing_cols:
                                    st.error(f"❌ Cannot process: Missing required columns: {', '.join(missing_cols)}")
                                else:
                                    process_google_sheet(processor, sheet_id, start_row, num_rows, sheet_name)
                        else:
                            st.info("👆 Please detect headers first to enable processing")
                            
                    else:
                        st.error("❌ Invalid Google Sheets URL")
                else:
                    st.info("👆 Please enter a Google Sheets URL to continue")
            
        except json.JSONDecodeError:
            st.error("❌ Invalid JSON credentials file")
        except Exception as e:
            st.error(f"❌ Error loading credentials: {e}")
    
    else:
        st.info("👆 Please upload your Desktop Application OAuth2 credentials to continue")
        st.warning("⚠️ Remember: Use Desktop Application credentials, not Web Application!")

def process_google_sheet(processor, sheet_id, start_row, num_rows, sheet_name):
    """Process Google Sheet with real-time updates"""
    
    # Initialize progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    results_container = st.empty()
    
    # Start time tracking
    start_time = time.time()
    
    # Progress callback function
    def update_progress(percentage, message):
        progress_bar.progress(percentage / 100)
        status_text.text(message)
    
    try:
        # Start processing
        with st.spinner("🔄 Initializing processing..."):
            results = processor.process_sheet_range(
                sheet_id=sheet_id,
                start_row=start_row,
                num_rows=num_rows,
                progress_callback=update_progress,
                sheet_name=sheet_name
            )
        
        # Display results
        if results.get("success"):
            # Success metrics
            progress_bar.progress(1.0)
            status_text.text("✅ Processing completed!")
            
            total_time = results["total_time"]
            
            with results_container.container():
                st.success("🎉 Processing completed successfully!")
                
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
        # Clean up progress indicators
        progress_bar.empty()
        status_text.empty()
