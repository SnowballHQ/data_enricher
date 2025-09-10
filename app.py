"""
Product Categorization and Cold Email Personalization System
Streamlit Frontend Application with Google Sheets Integration
"""

import streamlit as st
import pandas as pd
import os
import time
from utils.data_processor import DataProcessor
from utils.case_b_processor import CaseBProcessor
from utils.config_manager import ConfigManager
from utils.google_sheets_interface_persistent import render_google_sheets_section
from utils.background_ui import render_background_processing_section
from config import SUPPORTED_FORMATS

def health_check():
    """Health check endpoint for heartbeat system"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "app": "data-enricher",
        "version": "1.0.0"
    }

def main():
    # Check for health endpoint
    if st.query_params.get("health") == "check":
        health_data = health_check()
        st.json(health_data)
        return
    
    st.set_page_config(
        page_title="Product Categorization System",
        page_icon="🏷️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize configuration manager
    config_manager = ConfigManager()
    
    st.title("🏷️ Product Categorization & Cold Email Generator")
    st.markdown("---")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Check if OpenAI is already configured
        if config_manager.is_openai_configured():
            st.success("✅ OpenAI API Key configured")
            
            # Show current configuration
            with st.expander("🔧 Current Settings"):
                st.write(f"**Model:** {config_manager.get_openai_model()}")
                #st.write(f"**Max Tokens:** {config_manager.get_openai_max_tokens()}")
                #st.write(f"**Temperature:** {config_manager.get_openai_temperature()}")
                
                # Option to change API key
                if st.button("🔄 Change API Key"):
                    st.session_state.show_api_input = True
                
                # Export configuration
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("📁 Export .env"):
                        if config_manager.create_env_file():
                            st.success("✅ .env file created!")
                        else:
                            st.error("❌ Failed to create .env file")
                
                with col2:
                    if st.button("📋 Export Template"):
                        if config_manager.export_env_template():
                            st.success("✅ Template exported!")
                        else:
                            st.error("❌ Failed to export template")
        else:
            st.warning("⚠️ OpenAI API Key not configured")
            st.session_state.show_api_input = True
        
        # API Key input (shown when needed)
        if st.session_state.get('show_api_input', False) or not config_manager.is_openai_configured():
            st.subheader("🔑 Set OpenAI API Key")
            
            api_key = st.text_input(
                "OpenAI API Key",
                type="password",
                help="Enter your OpenAI API key to enable AI-powered categorization"
            )
            
            if st.button("💾 Save API Key"):
                if api_key and api_key.strip():
                    config_manager.set_openai_api_key(api_key.strip())
                    st.success("✅ API Key saved successfully!")
                    st.rerun()
                else:
                    st.error("❌ Please enter a valid API key")
            
            st.info("💡 Your API key will be saved locally and reused in future sessions")
        
        st.markdown("---")
        
        # Display supported formats
        st.subheader("📁 Supported File Formats")
        for format_type in SUPPORTED_FORMATS:
            st.write(f"• {format_type}")
        
        st.markdown("---")
        
        # Display categorization info
        st.subheader("🏷️ AI-Powered Categorization")
        st.info("""
        **OpenAI AI will automatically categorize your products based on:**
        • Product keywords and descriptions
        • Industry analysis and business context
        • Professional business categorization standards
        
        Categories are generated dynamically by AI - no predefined limits!
        """)
        
        st.markdown("---")
        
        # Instructions
        st.subheader("📋 Quick Start")
        st.markdown("""
        1. **Configure** your OpenAI API key (one-time setup)
        2. **Choose** processing method:
           - **File Upload**: Upload CSV/Excel files
           - **Google Sheets**: Real-time processing
        3. **Process** the data with AI categorization
        4. **Download** or view results
        """)
        
        # Column mapping info
        st.subheader("🔍 Column Requirements")
        st.markdown("""
        **Case A - Keywords + Description:**
        - **Keywords**: Company Keywords, keywords
        - **Description**: Company Short Description, description
        - **Company**: Company Name, name
        
        **Case B - Website URLs:**
        - **Website**: Website, URL, web
        - **Company**: Company Name, name
        """)
    
    # Main content area - check authentication
    if not config_manager.is_openai_configured():
        st.warning("⚠️ Please configure your OpenAI API key in the sidebar to continue")
        st.stop()
    
    # Get API key for processors
    api_key = config_manager.get_openai_api_key()
    
    # Main content with tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📁 File Upload", "🗒️ Google Sheets", "🔄 Background Processing", "ℹ️ System Info"])
    
    with tab1:
        render_file_upload_section(api_key, config_manager)
    
    with tab2:
        render_google_sheets_section(api_key)
    
    with tab3:
        render_background_processing_section(api_key)
    
    with tab4:
        render_system_info_section()

def render_file_upload_section(api_key: str, config_manager):
    """Render the file upload processing section"""
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📤 Upload Your File")
        
        uploaded_file = st.file_uploader(
            "Choose an Excel or CSV file",
            type=['xlsx', 'xls', 'csv'],
            help="Upload a file with columns for keywords and description"
        )
        
        if uploaded_file is not None:
            st.success(f"✅ File uploaded: {uploaded_file.name}")
            
            # Display file info
            file_size = len(uploaded_file.getvalue()) / 1024  # KB
            st.info(f"📊 File size: {file_size:.2f} KB")
            
            # Show preview of uploaded data
            try:
                if uploaded_file.name.endswith('.csv'):
                    preview_df = pd.read_csv(uploaded_file)
                else:
                    preview_df = pd.read_excel(uploaded_file)
                
                st.subheader("📋 Data Preview")
                st.dataframe(preview_df.head(), use_container_width=True)
                
                # Case A/B Selection
                st.subheader("🔄 Choose Your Data Type")
                
                processing_type = st.radio(
                    "What type of data do you want to process?",
                    options=["Case A: Keywords & Description", "Case B: Website URLs Only"],
                    help="Select based on what columns your file contains"
                )
                
                # Show requirements and example based on selection
                if "Case A" in processing_type:
                    st.info("📋 **Case A Requirements:**\n- Company name column\n- Keywords/tags column\n- Description/about column")
                    st.success("💡 **Example:** Your file should have columns like 'Company Name', 'Keywords', 'Description'")
                    case_type = "CASE_A"
                else:
                    st.info("🌐 **Case B Requirements:**\n- Company name column\n- Website URL column")
                    st.success("💡 **Example:** Your file should have columns like 'Company Name', 'Website'")
                    case_type = "CASE_B"
                
                st.markdown("---")
                
                # Show column mapping info
                st.subheader("🔍 Column Analysis")
                
                if case_type == "CASE_A":
                    # Check for keywords columns - specific to user's column names
                    keywords_found = []
                    for col in preview_df.columns:
                        col_lower = col.lower()
                        # Look for exact matches first
                        if col == "Company Keywords":
                            keywords_found.append(col)
                        # Then look for similar patterns
                        elif any(keyword in col_lower for keyword in ['keyword', 'key']):
                            keywords_found.append(col)
                    
                    if keywords_found:
                        st.success(f"✅ Keywords column found: {', '.join(keywords_found)}")
                    else:
                        st.warning("⚠️ No keywords column detected")
                    
                    # Check for description columns - specific to user's column names
                    description_found = []
                    for col in preview_df.columns:
                        col_lower = col.lower()
                        # Look for exact matches first
                        if col == "Company Short Description":
                            description_found.append(col)
                        # Then look for similar patterns
                        elif any(desc in col_lower for desc in ['description', 'desc', 'about', 'summary']):
                            description_found.append(col)
                    
                    if description_found:
                        st.success(f"✅ Description column found: {', '.join(description_found)}")
                    else:
                        st.warning("⚠️ No description column detected")
                    
                    # Check for company columns - specific to user's column names
                    company_found = []
                    for col in preview_df.columns:
                        col_lower = col.lower()
                        # Look for exact matches first
                        if col == "Company Name":
                            company_found.append(col)
                        # Then look for similar patterns
                        elif any(comp in col_lower for comp in ['company', 'name', 'brand', 'organization']):
                            company_found.append(col)
                    
                    if company_found:
                        st.success(f"✅ Company column found: {', '.join(company_found)}")
                    else:
                        st.info("ℹ️ No company column detected (will use generic placeholder)")
                    
                    # Debug: Show all columns for troubleshooting
                    st.subheader("🔍 All Columns in Your File")
                    st.write("**Available columns:**")
                    for i, col in enumerate(preview_df.columns):
                        # Highlight the columns we're looking for
                        if col in ["Company Keywords", "Company Short Description", "Company Name"]:
                            st.write(f"{i+1}. **{col}** ⭐ (Required)")
                        else:
                            st.write(f"{i+1}. {col}")
                    
                    # Check if we have the minimum required columns for processing
                    has_keywords = len(keywords_found) > 0
                    has_description = len(description_found) > 0
                    
                    # Only show success and process button if we actually found the columns
                    if has_keywords and has_description:
                        st.success("✅ All required columns found!")
                        
                        # Date filter option
                        st.subheader("📅 Date Filter (Optional)")
                        
                        # Check if Instantly Date column exists
                        has_instantly_date = 'Instantly Date' in preview_df.columns
                        
                        if has_instantly_date:
                            # Get unique dates from the column
                            unique_dates = preview_df['Instantly Date'].dropna().unique()
                            unique_dates = sorted([str(d) for d in unique_dates])
                            
                            st.info(f"📊 Found {len(unique_dates)} unique dates in 'Instantly Date' column")
                            
                            # Date selection options
                            filter_option = st.radio(
                                "Choose processing option:",
                                ["Process all rows", "Filter by specific date"],
                                key="date_filter_option"
                            )
                            
                            selected_date = None
                            if filter_option == "Filter by specific date":
                                selected_date = st.selectbox(
                                    "Select Instantly Date:",
                                    options=unique_dates,
                                    key="selected_instantly_date"
                                )
                                
                                if selected_date:
                                    matching_rows = len(preview_df[preview_df['Instantly Date'] == selected_date])
                                    st.info(f"📈 {matching_rows} rows will be processed with date '{selected_date}'")
                        else:
                            st.info("📅 'Instantly Date' column not found - will process all rows")
                            selected_date = None
                        
                        # Process button
                        if st.button("🚀 Process Data with AI", type="primary"):
                            # Double-check before processing
                            if verify_columns_for_processing(preview_df):
                                process_data(uploaded_file, config_manager, selected_date, case_type, api_key)
                            else:
                                st.error("❌ Column verification failed. Please check your file format.")
                    else:
                        missing_cols = []
                        if not has_keywords:
                            missing_cols.append("keywords (Company Keywords)")
                        if not has_description:
                            missing_cols.append("description (Company Short Description)")
                        
                        st.error(f"❌ Missing required columns: {', '.join(missing_cols)}")
                        st.info("Please ensure your file contains 'Company Keywords' and 'Company Short Description' columns")
                        
                        # Additional help
                        st.info("💡 **Required columns:** 'Company Keywords' and 'Company Short Description'")
                        st.info("🔍 **Found columns:** Check the list above to see what columns are actually in your file")
                else:
                    # Case B: Check for website and company columns
                    website_found = []
                    for col in preview_df.columns:
                        col_lower = col.lower()
                        # Look for exact matches first
                        if col in ["Website", "URL", "Web"]:
                            website_found.append(col)
                        # Then look for similar patterns
                        elif any(web in col_lower for web in ['website', 'url', 'web', 'link']):
                            website_found.append(col)
                    
                    if website_found:
                        st.success(f"✅ Website column found: {', '.join(website_found)}")
                    else:
                        st.warning("⚠️ No website column detected")
                    
                    # Check for company columns
                    company_found = []
                    for col in preview_df.columns:
                        col_lower = col.lower()
                        # Look for exact matches first
                        if col == "Company Name":
                            company_found.append(col)
                        # Then look for similar patterns
                        elif any(comp in col_lower for comp in ['company', 'name', 'brand', 'organization']):
                            company_found.append(col)
                    
                    if company_found:
                        st.success(f"✅ Company column found: {', '.join(company_found)}")
                    else:
                        st.info("ℹ️ No company column detected (will use generic placeholder)")
                    
                    # Debug: Show all columns for troubleshooting
                    st.subheader("🔍 All Columns in Your File")
                    st.write("**Available columns:**")
                    for i, col in enumerate(preview_df.columns):
                        # Highlight the columns we're looking for
                        if col in ["Website", "Company Name"]:
                            st.write(f"{i+1}. **{col}** ⭐ (Required)")
                        else:
                            st.write(f"{i+1}. {col}")
                    
                    # Check if we have the minimum required columns for processing
                    has_website = len(website_found) > 0
                    
                    # Only show success and process button if we actually found the columns
                    if has_website:
                        st.success("✅ All required columns found!")
                        
                        # Process button for Case B
                        if st.button("🚀 Process Data with AI", type="primary"):
                            process_data(uploaded_file, config_manager, None, case_type, api_key)
                    else:
                        st.error("❌ Missing required columns for Case B")
                        st.info("Please ensure your file contains a website/URL column")
                        
                        # Additional help
                        st.info("💡 **Required columns:** Website/URL column")
                        st.info("🔍 **Found columns:** Check the list above to see what columns are actually in your file")

                
            except Exception as e:
                st.error(f"❌ Error reading file: {str(e)}")
    
    with col2:
        st.subheader("📊 Processing Status")
        
        if 'processing_status' in st.session_state:
            st.info(st.session_state.processing_status)
        
        if 'processed_data' in st.session_state:
            st.success("✅ Data processing completed!")
            
            # Show summary
            df = st.session_state.processed_data
            st.metric("Total Rows", len(df))
            st.metric("Categories Assigned", df['category'].nunique())
            
            # Category distribution
            st.subheader("📈 Category Distribution")
            category_counts = df['category'].value_counts()
            st.bar_chart(category_counts)
            
            # Download options
            st.subheader("💾 Download Options")
            
            col_a, col_b = st.columns(2)
            
            with col_a:
                if st.button("📊 Download Excel"):
                    download_excel(df)
            
            with col_b:
                if st.button("📄 Download CSV"):
                    download_csv(df)

def verify_columns_for_processing(df):
    """Verify that the required columns exist for processing"""
    # Check for exact column names first
    has_keywords = "Company Keywords" in df.columns
    has_description = "Company Short Description" in df.columns
    
    # If exact names not found, check for similar patterns
    if not has_keywords:
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['keyword', 'key']):
                has_keywords = True
                break
    
    if not has_description:
        for col in df.columns:
            if any(desc in col.lower() for desc in ['description', 'desc', 'about', 'summary']):
                has_description = True
                break
    
    return has_keywords and has_description

def process_data(uploaded_file, config_manager, instantly_date=None, case_type="CASE_A", api_key=None):
    """Process the uploaded file with AI categorization"""
    
    # Initialize progress
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        if case_type == "CASE_A":
            # Initialize data processor for Case A (keywords + description)
            processor = DataProcessor(api_key)
            
            # Update status
            status_text.text("🔄 Reading file...")
            progress_bar.progress(20)
            
            # Update status
            status_text.text("🤖 Processing with AI...")
            progress_bar.progress(40)
            
            # Get file content first (before any other file operations)  
            file_content = uploaded_file.getvalue()
        else:
            # Initialize Case B processor for website scraping
            processor = CaseBProcessor(api_key)
            
            # Update status
            status_text.text("🔄 Reading file...")
            progress_bar.progress(20)
            
            # Update status
            status_text.text("🌐 Scraping websites...")
            progress_bar.progress(40)
            
            # Get file content first (before any other file operations)  
            file_content = uploaded_file.getvalue()
        
        # Reset file pointer and read for column validation
        uploaded_file.seek(0)
        if uploaded_file.name.endswith('.csv'):
            df_check = pd.read_csv(uploaded_file)
        else:
            df_check = pd.read_excel(uploaded_file)
        
        if case_type == "CASE_A":
            # Check if the required columns exist for Case A
            required_cols = ['Company Keywords', 'Company Short Description']
            missing_cols = [col for col in required_cols if col not in df_check.columns]
            
            if missing_cols:
                st.error(f"❌ Missing required columns: {', '.join(missing_cols)}")
                st.info("Please ensure your file contains 'Company Keywords' and 'Company Short Description' columns")
                return
            
            # Process the data with Case A processor
            enriched_df, error_msg = processor.process_file(file_content, uploaded_file.name, instantly_date)
            
            if error_msg:
                st.error(f"❌ {error_msg}")
                return
        else:
            # Case B: Check if the required columns exist for website processing
            has_website = False
            for col in df_check.columns:
                col_lower = col.lower()
                if any(web in col_lower for web in ['website', 'url', 'web', 'link']):
                    has_website = True
                    break
            
            if not has_website:
                st.error("❌ Missing website column")
                st.info("Please ensure your file contains a website/URL column")
                return
            
            # Process the data with Case B processor
            try:
                enriched_df = processor.process_dataframe(df_check)
                error_msg = None
            except Exception as e:
                st.error(f"❌ Error processing websites: {str(e)}")
                return
        
        # Update status
        status_text.text("✅ Processing completed!")
        progress_bar.progress(100)
        
        # Store in session state
        st.session_state.processed_data = enriched_df
        st.session_state.processing_status = "Data processing completed successfully!"
        
        # Show success message
        st.success("🎉 Data processing completed successfully!")
        
        # Display results
        st.subheader("📊 Processing Results")
        st.dataframe(enriched_df, use_container_width=True)
        
        # Show category summary
        st.subheader("🏷️ Category Summary")
        category_summary = enriched_df['category'].value_counts()
        st.dataframe(category_summary.reset_index().rename(columns={'index': 'Category', 'category': 'Count'}))
        
    except Exception as e:
        st.error(f"❌ Error during processing: {str(e)}")
        status_text.text("❌ Processing failed")
    
    finally:
        # Clean up
        progress_bar.empty()
        status_text.empty()

def download_excel(df):
    """Download the processed data as Excel file"""
    try:
        processor = DataProcessor("dummy_key")  # Key not needed for export
        excel_data = processor.export_to_excel(df)
        
        st.download_button(
            label="📥 Download Excel File",
            data=excel_data,
            file_name="enriched_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        st.error(f"❌ Error creating Excel file: {str(e)}")

def download_csv(df):
    """Download the processed data as CSV file"""
    try:
        processor = DataProcessor("dummy_key")  # Key not needed for export
        csv_data = processor.export_to_csv(df)
        
        st.download_button(
            label="📥 Download CSV File",
            data=csv_data,
            file_name="enriched_data.csv",
            mime="text/csv"
        )
    except Exception as e:
        st.error(f"❌ Error creating CSV file: {str(e)}")

def render_system_info_section():
    """System information and configuration section"""
    st.header("ℹ️ System Information")
    
    st.subheader("🔧 System Status")
    col1, col2 = st.columns(2)
    
    with col1:
        api_status = "✅ Available" if os.getenv('OPENAI_API_KEY') else "❌ Not configured"
        st.info(f"**OpenAI API:** {api_status}")
    
    with col2:
        st.info(f"**Processing Modes:** File Upload + Google Sheets")
    
    st.subheader("💰 Cost Information")
    st.info("""
    **Estimated costs with GPT-4.1-nano:**
    - 100 brands: ~$0.009 (less than 1 cent!)
    - 1,000 brands: ~$0.09
    - 10,000 brands: ~$0.90
    """)
    
    st.subheader("🚀 Features")
    st.success("""
    ✅ **Real-time Google Sheets processing**
    ✅ **Row-by-row progress tracking**
    ✅ **Resumable processing (start from any row)**
    ✅ **Error handling with skip logic**
    ✅ **OAuth authentication**
    ✅ **Concurrent file processing**
    ✅ **AI-powered categorization**
    ✅ **Email question generation**
    """)

if __name__ == "__main__":
    main()
