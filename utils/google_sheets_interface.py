"""
Streamlit interface for Google Sheets processing
"""

import streamlit as st
import json
import time
from utils.google_sheets_processor import GoogleSheetsProcessor

def render_google_sheets_section(api_key: str):
    """Render the Google Sheets processing section in Streamlit"""
    
    st.header("🗒️ Google Sheets Real-time Processing")
    
    # Instructions
    with st.expander("📋 Setup Instructions"):
        st.markdown("""
        **Prerequisites:**
        1. **Google Cloud Project** with Sheets API enabled
        2. **OAuth2 credentials** (client_secret.json)
        3. **Google Sheet** with edit permissions
        
        **Sheet Format (Columns A-G):**
        - **A:** Company Name
        - **B:** Keywords  
        - **C:** Description
        - **D:** Category (will be filled)
        - **E:** Brand Name (will be filled)
        - **F:** Email Question (will be filled)
        - **G:** Status (will be filled)
        """)
    
    # OAuth2 Credentials Upload
    st.subheader("🔑 Authentication")
    
    credentials_file = st.file_uploader(
        "Upload OAuth2 Credentials (client_secret.json)",
        type=['json'],
        help="Download from Google Cloud Console > APIs & Services > Credentials"
    )
    
    if credentials_file is not None:
        try:
            credentials_json = json.load(credentials_file)
            
            # Initialize processor
            if 'sheets_processor' not in st.session_state:
                st.session_state.sheets_processor = GoogleSheetsProcessor(api_key)
            
            processor = st.session_state.sheets_processor
            
            # Authenticate
            if processor.authenticate_oauth(credentials_json):
                st.success("✅ Authenticated with Google Sheets!")
                
                # Processing options
                st.subheader("📊 Processing Options")
                
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
                
                col3, col4, col5 = st.columns(3)
                
                with col3:
                    start_row = st.number_input(
                        "Start Row",
                        min_value=1,
                        value=2,
                        help="Row number to start processing (2 to skip header)"
                    )
                
                with col4:
                    num_rows = st.number_input(
                        "Rows to Process",
                        min_value=1,
                        value=10,
                        max_value=1000,
                        help="Number of rows to process"
                    )
                
                with col5:
                    update_frequency = st.selectbox(
                        "Update Frequency",
                        options=[1, 5, 10],
                        index=0,
                        help="Update progress every N rows"
                    )
                
                # Validation
                if sheet_url:
                    sheet_id = processor.extract_sheet_id(sheet_url)
                    
                    if sheet_id:
                        st.success(f"✅ Valid sheet ID: {sheet_id[:10]}...")
                        
                        # Preview section
                        if st.button("👀 Preview Data", help="Preview the data that will be processed"):
                            with st.spinner("Fetching preview..."):
                                preview_df = processor.get_sheet_data(sheet_id, start_row, min(5, num_rows), sheet_name)
                                
                                if preview_df is not None:
                                    st.subheader("📋 Data Preview")
                                    st.dataframe(preview_df.head(), use_container_width=True)
                                    
                                    # Show column mapping
                                    mapped_df = processor._map_columns(preview_df.copy())
                                    
                                    st.subheader("🔍 Column Mapping")
                                    col_map_col1, col_map_col2, col_map_col3 = st.columns(3)
                                    
                                    with col_map_col1:
                                        st.info(f"**Keywords:** {_find_mapped_column(preview_df, 'keywords')}")
                                    with col_map_col2:
                                        st.info(f"**Description:** {_find_mapped_column(preview_df, 'description')}")
                                    with col_map_col3:
                                        st.info(f"**Company:** {_find_mapped_column(preview_df, 'company_name')}")
                        
                        # Main processing button
                        st.markdown("---")
                        
                        if st.button("🚀 Start Processing", type="primary", use_container_width=True):
                            process_google_sheet(processor, sheet_id, start_row, num_rows, sheet_name, update_frequency)
                            
                    else:
                        st.error("❌ Invalid Google Sheets URL")
                else:
                    st.info("👆 Please enter a Google Sheets URL to continue")
            
        except json.JSONDecodeError:
            st.error("❌ Invalid JSON credentials file")
        except Exception as e:
            st.error(f"❌ Error loading credentials: {e}")
    
    else:
        st.info("👆 Please upload your OAuth2 credentials to continue")

def process_google_sheet(processor, sheet_id, start_row, num_rows, sheet_name, update_frequency):
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
                
                # Skipped rows info
                if results["skipped_rows"]:
                    with st.expander(f"⚠️ Skipped Rows ({len(results['skipped_rows'])})"):
                        st.write("Row numbers with errors or empty data:")
                        st.write(", ".join(map(str, results["skipped_rows"])))
                
                st.info("💡 Check your Google Sheet to see the real-time results!")
        
        else:
            st.error(f"❌ Processing failed: {results.get('error', 'Unknown error')}")
    
    except Exception as e:
        st.error(f"❌ Processing error: {str(e)}")
    
    finally:
        # Clean up progress indicators
        progress_bar.empty()
        status_text.empty()

def _find_mapped_column(df, target_col):
    """Helper function to find which column was mapped"""
    # This is a simplified version - the actual mapping is in the processor
    column_mappings = {
        'keywords': ['Company Keywords', 'keywords', 'Keywords', 'tags', 'Tags'],
        'description': ['Company Short Description', 'description', 'Description', 'about', 'About'],
        'company_name': ['Company Name', 'company_name', 'name', 'Name', 'brand', 'Brand']
    }
    
    for col in column_mappings.get(target_col, []):
        if col in df.columns:
            return col
    
    return "❌ Not found"
