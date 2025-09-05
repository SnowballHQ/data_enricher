import streamlit as st
import pandas as pd
import os
import logging
import tempfile
from datetime import datetime
from app.file_processor import FileProcessor
from app.case_a_processor import CaseAProcessor
from app.case_b_processor import CaseBProcessor
from config.settings import SUPPORTED_FORMATS, MAX_FILE_SIZE_MB, OUTPUT_COLUMNS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/app.log'),
        logging.StreamHandler()
    ]
)

def main():
    st.set_page_config(
        page_title="Data Enrichment & Cold Email System",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Data Enrichment & Cold Email Personalization System")
    st.markdown("---")
    
    # Initialize session state
    if 'processing_complete' not in st.session_state:
        st.session_state.processing_complete = False
    if 'processed_df' not in st.session_state:
        st.session_state.processed_df = None
    if 'original_filename' not in st.session_state:
        st.session_state.original_filename = None
    
    # Sidebar with instructions
    with st.sidebar:
        st.header("📋 Instructions")
        st.markdown("""
        **Step 1:** Upload your CSV or Excel file
        
        **Case A:** File contains `keywords` and `description` columns
        - System will directly process the text
        
        **Case B:** File contains only `website` column
        - System will scrape website content automatically
        
        **Step 2:** Select processing type (auto-detected)
        
        **Step 3:** Start processing
        
        **Step 4:** Download enriched file
        """)
        
        st.markdown("---")
        st.markdown("**Supported Formats:**")
        for fmt in SUPPORTED_FORMATS:
            st.markdown(f"• {fmt.upper()}")
        
        st.markdown(f"**Max File Size:** {MAX_FILE_SIZE_MB}MB")
    
    # Main content
    tab1, tab2, tab3 = st.tabs(["📁 Upload & Process", "📊 Results", "ℹ️ System Info"])
    
    with tab1:
        file_upload_section()
    
    with tab2:
        results_section()
    
    with tab3:
        system_info_section()

def file_upload_section():
    """File upload and processing section"""
    st.header("📁 File Upload & Processing")
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Choose a CSV or Excel file",
        type=[fmt.replace('.', '') for fmt in SUPPORTED_FORMATS],
        help=f"Maximum file size: {MAX_FILE_SIZE_MB}MB"
    )
    
    if uploaded_file is not None:
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{uploaded_file.name.split('.')[-1]}") as temp_file:
            temp_file.write(uploaded_file.read())
            temp_file_path = temp_file.name
        
        try:
            # Load and display basic file info
            try:
                df_preview = pd.read_csv(temp_file_path) if temp_file_path.endswith('.csv') else pd.read_excel(temp_file_path)
                file_size_mb = os.path.getsize(temp_file_path) / (1024 * 1024)
                
                # Display file metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("File Size", f"{file_size_mb:.2f} MB")
                with col2:
                    st.metric("Rows", len(df_preview))
                with col3:
                    st.metric("Columns", len(df_preview.columns))
                
                # Basic file validation
                if file_size_mb > MAX_FILE_SIZE_MB:
                    st.error(f"❌ File too large: {file_size_mb:.2f}MB (max: {MAX_FILE_SIZE_MB}MB)")
                    return
                
                st.success("✅ File uploaded successfully!")
                
                # Display column preview
                st.subheader("📋 Column Preview")
                st.dataframe(df_preview.head(), use_container_width=True)
                
                # Show available columns
                st.subheader("📋 Available Columns")
                st.write("Your file contains these columns:")
                for i, col in enumerate(df_preview.columns, 1):
                    st.write(f"{i}. **{col}**")
                
                # Processing type selection
                st.subheader("🔄 Choose Processing Type")
                
                processing_type = st.radio(
                    "What type of data do you have?",
                    options=["Case A: Keywords & Description", "Case B: Website URLs Only"],
                    help="Choose based on what columns your file contains"
                )
                
                # Show requirements based on selection
                if "Case A" in processing_type:
                    st.info("📋 **Case A Requirements:**\n- Company name column\n- Keywords/tags column\n- Description/about column")
                    case_type = "CASE_A"
                else:
                    st.info("🌐 **Case B Requirements:**\n- Company name column\n- Website URL column")
                    case_type = "CASE_B"
                
                # Processing options
                st.subheader("⚙️ Processing Options")
                col1, col2 = st.columns(2)
                
                with col1:
                    use_parallel = st.checkbox("Use parallel processing", value=True, help="Faster for large files")
                
                with col2:
                    if case_type == "CASE_B":
                        scrape_timeout = st.slider("Scraping timeout (minutes)", 1, 10, 5)
                
                # Process button
                if st.button("🚀 Start Processing", type="primary", use_container_width=True):
                    process_file(temp_file_path, case_type, use_parallel, uploaded_file.name)
                    
            except Exception as e:
                st.error(f"❌ Error reading file: {e}")
                return
        
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_file_path)
            except:
                pass

def process_file(file_path: str, case_type: str, use_parallel: bool, original_filename: str):
    """Process the uploaded file"""
    try:
        # Load data directly without file processor validation
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        # Clean data
        df = df.dropna(how='all')  # Remove completely empty rows
        df = df.reset_index(drop=True)
        
        # Create progress bars
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_progress(progress: float, message: str):
            progress_bar.progress(min(progress / 100, 1.0))
            status_text.text(message)
        
        # Process based on case type
        if case_type == "CASE_A":
            st.info("🔄 Processing with keywords and description...")
            processor = CaseAProcessor()
            
            if use_parallel:
                processed_df = processor.process_dataframe_parallel(
                    df, max_workers=4, progress_callback=update_progress
                )
            else:
                processed_df = processor.process_dataframe(
                    df, progress_callback=update_progress
                )
        
        elif case_type == "CASE_B":
            st.info("🔄 Processing with web scraping...")
            processor = CaseBProcessor()
            processed_df = processor.process_dataframe(
                df, progress_callback=update_progress
            )
        
        else:
            st.error("Unknown processing case type")
            return
        
        # Update progress to complete
        progress_bar.progress(1.0)
        status_text.text("✅ Processing completed!")
        
        # Store results in session state
        st.session_state.processed_df = processed_df
        st.session_state.processing_complete = True
        st.session_state.original_filename = original_filename
        
        # Display summary
        success_count = len(processed_df[processed_df['processing_status'] == 'success'])
        error_count = len(processed_df[processed_df['processing_status'] == 'error'])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("✅ Successful", success_count)
        with col2:
            st.metric("❌ Errors", error_count)
        with col3:
            st.metric("📊 Success Rate", f"{(success_count / len(processed_df) * 100):.1f}%")
        
        st.success("🎉 Processing completed! Check the Results tab to download your file.")
        
        # Auto-switch to results tab
        st.rerun()
        
    except Exception as e:
        st.error(f"❌ Error during processing: {str(e)}")
        logging.error(f"Processing error: {e}")

def results_section():
    """Results display and download section"""
    st.header("📊 Processing Results")
    
    if not st.session_state.processing_complete or st.session_state.processed_df is None:
        st.info("📋 No processed data available. Please upload and process a file first.")
        return
    
    df = st.session_state.processed_df
    
    # Summary metrics
    st.subheader("📈 Processing Summary")
    col1, col2, col3, col4 = st.columns(4)
    
    total_rows = len(df)
    success_rows = len(df[df['processing_status'] == 'success'])
    error_rows = len(df[df['processing_status'] == 'error'])
    success_rate = (success_rows / total_rows * 100) if total_rows > 0 else 0
    
    with col1:
        st.metric("Total Rows", total_rows)
    with col2:
        st.metric("✅ Successful", success_rows)
    with col3:
        st.metric("❌ Errors", error_rows)
    with col4:
        st.metric("Success Rate", f"{success_rate:.1f}%")
    
    # Category distribution
    if 'category' in df.columns:
        st.subheader("📊 Category Distribution")
        category_counts = df[df['processing_status'] == 'success']['category'].value_counts()
        if not category_counts.empty:
            st.bar_chart(category_counts)
    
    # Data preview
    st.subheader("👀 Data Preview")
    
    # Filter options
    col1, col2 = st.columns(2)
    with col1:
        status_filter = st.selectbox("Filter by Status", ["All", "Success", "Error"])
    with col2:
        show_columns = st.multiselect(
            "Select Columns to Display",
            df.columns.tolist(),
            default=['company_name', 'category', 'brand_name', 'processing_status'][:len(df.columns)]
        )
    
    # Apply filters
    filtered_df = df.copy()
    if status_filter == "Success":
        filtered_df = df[df['processing_status'] == 'success']
    elif status_filter == "Error":
        filtered_df = df[df['processing_status'] == 'error']
    
    if show_columns:
        display_df = filtered_df[show_columns]
    else:
        display_df = filtered_df
    
    # Display data
    st.dataframe(display_df, use_container_width=True, height=400)
    
    # Download section
    st.subheader("💾 Download Results")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Excel download
        excel_buffer = generate_excel_download(df)
        st.download_button(
            label="📊 Download as Excel",
            data=excel_buffer,
            file_name=f"enriched_{st.session_state.original_filename.rsplit('.', 1)[0]}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    with col2:
        # CSV download
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="📄 Download as CSV",
            data=csv_data,
            file_name=f"enriched_{st.session_state.original_filename.rsplit('.', 1)[0]}.csv",
            mime="text/csv",
            use_container_width=True
        )

def generate_excel_download(df: pd.DataFrame) -> bytes:
    """Generate Excel file for download"""
    from io import BytesIO
    
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Enriched Data', index=False)
        
        # Get workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets['Enriched Data']
        
        # Auto-adjust column widths
        for column_cells in worksheet.columns:
            length = max(len(str(cell.value or "")) for cell in column_cells)
            worksheet.column_dimensions[column_cells[0].column_letter].width = min(length + 2, 50)
    
    buffer.seek(0)
    return buffer.read()

def system_info_section():
    """System information and configuration section"""
    st.header("ℹ️ System Information")
    
    # System status
    st.subheader("🔧 System Status")
    col1, col2 = st.columns(2)
    
    with col1:
        # Check OpenAI API
        api_status = "✅ Available" if os.getenv('OPENAI_API_KEY') else "❌ Not configured"
        st.info(f"**OpenAI API:** {api_status}")
        
        # Check Scrapy
        try:
            import scrapy
            scrapy_status = f"✅ Version {scrapy.__version__}"
        except ImportError:
            scrapy_status = "❌ Not installed"
        st.info(f"**Scrapy:** {scrapy_status}")
    
    with col2:
        st.info(f"**Max File Size:** {MAX_FILE_SIZE_MB}MB")
        st.info(f"**Supported Formats:** {', '.join(SUPPORTED_FORMATS)}")
    
    # Configuration
    st.subheader("⚙️ Configuration")
    
    with st.expander("📋 Product Categories"):
        import json
        try:
            with open('config/categories.json', 'r') as f:
                categories = json.load(f)
            
            for category, data in categories.items():
                st.write(f"**{category}:** {data.get('description', '')}")
        except FileNotFoundError:
            st.error("Categories configuration file not found")
    
    with st.expander("🌐 Scraping Settings"):
        st.write("**Pages scraped per website:**")
        from config.settings import PAGES_TO_SCRAPE
        for page in PAGES_TO_SCRAPE:
            st.write(f"• {page}")
        
        st.write("**Content ignored:**")
        from config.settings import IGNORE_SELECTORS
        for selector in IGNORE_SELECTORS[:5]:  # Show first 5
            st.write(f"• {selector}")
        if len(IGNORE_SELECTORS) > 5:
            st.write(f"• ... and {len(IGNORE_SELECTORS) - 5} more")
    
    # Logs
    st.subheader("📋 Recent Logs")
    try:
        if os.path.exists('logs/app.log'):
            with open('logs/app.log', 'r') as f:
                logs = f.readlines()
            
            # Show last 20 lines
            recent_logs = logs[-20:] if len(logs) > 20 else logs
            log_text = ''.join(recent_logs)
            st.code(log_text, language='text')
        else:
            st.info("No log file found")
    except Exception as e:
        st.error(f"Error reading logs: {e}")

if __name__ == "__main__":
    main()