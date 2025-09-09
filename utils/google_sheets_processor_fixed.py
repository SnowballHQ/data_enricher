"""
Google Sheets Integration for Real-time Data Processing
Fixed version with proper header detection and column management
"""

import os
import json
import time
import re
from typing import Optional, Dict, List, Tuple
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
from utils.openai_categorizer import OpenAICategorizer
from utils.google_auth_manager import GoogleAuthManager

class GoogleSheetsProcessor:
    """Google Sheets processor with OAuth authentication and real-time updates"""
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    def __init__(self, api_key: str):
        """Initialize with OpenAI API key"""
        self.categorizer = OpenAICategorizer(api_key)
        self.auth_manager = GoogleAuthManager()
        self.service = None
        self.headers = None
        self.header_row = 1  # Default header row
        
        # Setup gitignore for credential files
        self.auth_manager.setup_gitignore()
        
    def is_authenticated(self) -> bool:
        """Check if already authenticated"""
        if self.auth_manager.is_authenticated():
            if not self.service:
                credentials = self.auth_manager.get_credentials()
                self.service = build('sheets', 'v4', credentials=credentials)
            return True
        return False
    
    def authenticate_oauth(self, credentials_json: dict = None) -> bool:
        """
        Authenticate with Google Sheets using persistent OAuth
        
        Args:
            credentials_json: OAuth2 client credentials (only needed for first time)
            
        Returns:
            bool: True if authentication successful
        """
        try:
            # Check if already authenticated
            if self.is_authenticated():
                return True
            
            # If no credentials provided, try to load from disk
            if not credentials_json:
                credentials_json = self.auth_manager.load_client_credentials()
                if not credentials_json:
                    return False  # Need to upload credentials first
            
            # Start new authentication flow
            if self.auth_manager.authenticate_new(credentials_json):
                return True  # Flow started, waiting for auth code
            else:
                return False
                
        except Exception as e:
            st.error(f"❌ Authentication error: {e}")
            return False
    
    def complete_authentication(self, auth_code: str) -> bool:
        """Complete authentication with authorization code"""
        if self.auth_manager.complete_authentication(auth_code):
            # Build service after successful authentication
            credentials = self.auth_manager.get_credentials()
            if credentials:
                self.service = build('sheets', 'v4', credentials=credentials)
                return True
        return False
    
    def revoke_authentication(self) -> bool:
        """Revoke stored authentication"""
        self.service = None
        return self.auth_manager.revoke_authentication()
    
    def get_auth_status(self) -> dict:
        """Get authentication status"""
        return self.auth_manager.get_auth_status()
    
    def extract_sheet_id(self, sheet_url: str) -> Optional[str]:
        """Extract Google Sheets ID from URL"""
        try:
            # Pattern to match Google Sheets URL
            pattern = r'/spreadsheets/d/([a-zA-Z0-9-_]+)'
            match = re.search(pattern, sheet_url)
            
            if match:
                return match.group(1)
            else:
                st.error("❌ Invalid Google Sheets URL format")
                return None
                
        except Exception as e:
            st.error(f"❌ Error extracting sheet ID: {e}")
            return None
    
    def detect_headers(self, sheet_id: str, sheet_name: str = "Sheet1") -> Optional[Dict]:
        """
        Detect headers in the sheet - always look at row 1 for headers
        Check if enriched columns already exist and reuse them
        
        Args:
            sheet_id: Google Sheets ID
            sheet_name: Name of the sheet tab
            
        Returns:
            Dict with header information
        """
        try:
            if not self.service:
                st.error("❌ Not authenticated with Google Sheets")
                return None
            
            # Always get headers from row 1 - expand range to check for existing enriched columns
            range_name = f"{sheet_name}!A1:Z1"
            
            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values or not values[0]:
                st.warning("⚠️ No headers found in row 1")
                return None
            
            headers = values[0]
            self.headers = headers
            self.header_row = 1
            
            # Map existing input columns
            column_mapping = self._map_input_columns(headers)
            
            # Check if enriched columns already exist
            enriched_columns = self._find_or_create_enriched_columns(headers)
            
            return {
                'headers': headers,
                'header_row': 1,
                'column_mapping': column_mapping,
                'enriched_columns': enriched_columns,
                'last_col_index': len(headers),
                'existing_enriched': self._has_existing_enriched_columns(headers)
            }
            
        except Exception as e:
            st.error(f"❌ Error detecting headers: {e}")
            return None
    
    def _find_or_create_enriched_columns(self, headers: List[str]) -> Dict[str, str]:
        """Find existing enriched columns or determine where to create new ones"""
        enriched_columns = {}
        
        # Look for existing enriched columns by name
        for i, header in enumerate(headers):
            header_lower = header.lower().strip()
            col_letter = chr(ord('A') + i)
            
            if header_lower in ['category']:
                enriched_columns['category'] = col_letter
            elif header_lower in ['brand name', 'brand_name', 'brandname']:
                enriched_columns['brand_name'] = col_letter
            elif header_lower in ['email question', 'email_question', 'emailquestion']:
                enriched_columns['email_question'] = col_letter
            elif header_lower in ['status']:
                enriched_columns['status'] = col_letter
        
        # If any enriched columns are missing, assign new positions
        required_columns = ['category', 'brand_name', 'email_question', 'status']
        last_col_index = len(headers)
        
        for i, col_name in enumerate(required_columns):
            if col_name not in enriched_columns:
                # Find next available column
                new_col_index = last_col_index + i
                enriched_columns[col_name] = chr(ord('A') + new_col_index)
        
        return enriched_columns
    
    def _has_existing_enriched_columns(self, headers: List[str]) -> bool:
        """Check if sheet already has enriched columns"""
        enriched_headers = ['category', 'brand name', 'brand_name', 'email question', 'email_question', 'status']
        
        for header in headers:
            if header.lower().strip() in enriched_headers:
                return True
        
        return False
    
    def _map_input_columns(self, headers: List[str]) -> Dict[str, str]:
        """Map input columns based on header names - supports both Case A and Case B"""
        column_mapping = {}
        
        # Keywords mapping (Case A)
        keywords_patterns = ['Company Keywords', 'keywords', 'Keywords', 'tags', 'Tags', 'keyword']
        for i, header in enumerate(headers):
            if header in keywords_patterns or any(pat.lower() in header.lower() for pat in ['keyword', 'tag']):
                column_mapping['keywords'] = chr(ord('A') + i)
                break
        
        # Description mapping (Case A)
        desc_patterns = ['Company Short Description', 'description', 'Description', 'about', 'About', 'summary']
        for i, header in enumerate(headers):
            if header in desc_patterns or any(pat.lower() in header.lower() for pat in ['description', 'desc', 'about', 'summary']):
                column_mapping['description'] = chr(ord('A') + i)
                break
        
        # Website mapping (Case B)
        website_patterns = ['Website', 'website', 'URL', 'url', 'Web', 'web', 'Link', 'link', 'Homepage', 'homepage']
        for i, header in enumerate(headers):
            if header in website_patterns or any(pat.lower() in header.lower() for pat in ['website', 'url', 'web', 'link', 'homepage']):
                column_mapping['website'] = chr(ord('A') + i)
                break
        
        # Company name mapping (both Case A and Case B)
        company_patterns = ['Company Name', 'company_name', 'name', 'Name', 'brand', 'Brand', 'company']
        for i, header in enumerate(headers):
            if header in company_patterns or any(pat.lower() in header.lower() for pat in ['company', 'name', 'brand']):
                column_mapping['company_name'] = chr(ord('A') + i)
                break
        
        return column_mapping
    
    def _process_case_b_row(self, website: str, company_name: str, row_number: int, 
                           enriched_columns: Dict[str, str], sheet_id: str, sheet_name: str) -> Dict[str, str]:
        """Process a single row for Case B (website + company) with scrapy + OpenAI"""
        try:
            # Initialize Case B processor for scraping
            from utils.case_b_processor import CaseBProcessor
            case_b_processor = CaseBProcessor(self.categorizer.api_key)
            
            # Create a simple DataFrame for the Case B processor
            import pandas as pd
            row_df = pd.DataFrame([{
                'Website': website,
                'Company Name': company_name if company_name else 'Unknown Company'
            }])
            
            # Process with scrapy + OpenAI
            processed_df = case_b_processor.process_dataframe(row_df)
            
            if len(processed_df) > 0:
                result_row = processed_df.iloc[0]
                
                # Extract results
                category = result_row.get('category', 'Unknown Category')
                brand_name = result_row.get('brand_name', company_name if company_name else 'Unknown Brand')
                email_question = result_row.get('email_question', 'What are the best local service providers?')
                
                # Check processing status
                processing_status = result_row.get('processing_status', 'unknown')
                if processing_status == 'error':
                    # Update with error status
                    self.update_row_status(sheet_id, row_number, f"❌ Error: {category}", enriched_columns, sheet_name)
                    return {
                        'category': 'Error',
                        'brand_name': brand_name,
                        'email_question': 'Error processing website'
                    }
                else:
                    # Success
                    return {
                        'category': category,
                        'brand_name': brand_name,
                        'email_question': email_question
                    }
            else:
                # No results returned
                self.update_row_status(sheet_id, row_number, "❌ No data processed", enriched_columns, sheet_name)
                return {
                    'category': 'Unknown Category',
                    'brand_name': company_name if company_name else 'Unknown Brand',
                    'email_question': 'What are the best local service providers?'
                }
                
        except Exception as e:
            # Handle any processing errors
            error_msg = str(e)[:50]
            self.update_row_status(sheet_id, row_number, f"❌ Error: {error_msg}", enriched_columns, sheet_name)
            return {
                'category': 'Error',
                'brand_name': company_name if company_name else 'Unknown Brand',
                'email_question': 'Error processing website'
            }
    
    def _detect_processing_case(self, column_mapping: Dict[str, str]) -> str:
        """Detect whether to use Case A (keywords+description) or Case B (company+website)"""
        has_keywords = 'keywords' in column_mapping
        has_description = 'description' in column_mapping
        has_website = 'website' in column_mapping
        has_company = 'company_name' in column_mapping
        
        if has_keywords and has_description:
            return "CASE_A"
        elif has_website and has_company:
            return "CASE_B"
        elif has_website:  # Website without company name still valid for Case B
            return "CASE_B"
        else:
            return "UNKNOWN"
    
    def setup_enriched_headers(self, sheet_id: str, enriched_columns: Dict[str, str], 
                              existing_enriched: bool, sheet_name: str = "Sheet1"):
        """Add headers for enriched data columns if they don't exist"""
        try:
            if not self.service:
                return False
            
            # If enriched columns already exist, don't modify headers
            if existing_enriched:
                st.info("ℹ️ Using existing enriched data columns")
                return True
            
            # Only add headers for new columns
            headers_to_add = []
            ranges_to_update = []
            
            # Check each enriched column individually
            for col_name, col_letter in enriched_columns.items():
                range_name = f"{sheet_name}!{col_letter}1"
                
                try:
                    result = self.service.spreadsheets().values().get(
                        spreadsheetId=sheet_id,
                        range=range_name
                    ).execute()
                    
                    existing_value = result.get('values', [])
                    
                    # If cell is empty, add header
                    if not existing_value or not existing_value[0] or not existing_value[0][0].strip():
                        header_name = {
                            'category': 'Category',
                            'brand_name': 'Brand Name', 
                            'email_question': 'Email Question',
                            'status': 'Status'
                        }.get(col_name, col_name.title())
                        
                        # Update this specific cell
                        body = {'values': [[header_name]]}
                        self.service.spreadsheets().values().update(
                            spreadsheetId=sheet_id,
                            range=range_name,
                            valueInputOption='RAW',
                            body=body
                        ).execute()
                        
                        headers_to_add.append(header_name)
                
                except Exception as e:
                    print(f"Error checking column {col_letter}: {e}")
            
            if headers_to_add:
                st.success(f"✅ Added new enriched headers: {', '.join(headers_to_add)}")
            else:
                st.info("ℹ️ All enriched headers already exist")
            
            return True
            
        except Exception as e:
            print(f"Error setting up headers: {e}")
            return False
    
    def get_sheet_data(self, sheet_id: str, start_row: int, num_rows: int, 
                      column_mapping: Dict[str, str], sheet_name: str = "Sheet1") -> Optional[pd.DataFrame]:
        """
        Get data from Google Sheets starting from specified row
        
        Args:
            sheet_id: Google Sheets ID
            start_row: Starting row number (1-based, data rows not header)
            num_rows: Number of rows to fetch
            column_mapping: Mapping of columns from header detection
            sheet_name: Name of the sheet tab
            
        Returns:
            DataFrame with the data
        """
        try:
            if not self.service:
                st.error("❌ Not authenticated with Google Sheets")
                return None
            
            # Calculate range for data (skip header row)
            data_start_row = max(2, start_row)  # Never start before row 2 (after headers)
            end_row = data_start_row + num_rows - 1
            range_name = f"{sheet_name}!A{data_start_row}:Z{end_row}"
            
            # Get values
            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                st.warning("⚠️ No data found in the specified range")
                return None
            
            # Create DataFrame with proper column mapping
            df = pd.DataFrame(values)
            
            # Map columns based on headers
            mapped_data = []
            for i, row in enumerate(values):
                row_data = {
                    'row_number': data_start_row + i,  # Track actual row number
                    'keywords': '',
                    'description': '',
                    'company_name': '',
                    'website': ''
                }
                
                # Extract data based on column mapping
                if 'keywords' in column_mapping:
                    col_index = ord(column_mapping['keywords']) - ord('A')
                    if col_index < len(row):
                        row_data['keywords'] = row[col_index] if row[col_index] else ''
                
                if 'description' in column_mapping:
                    col_index = ord(column_mapping['description']) - ord('A')
                    if col_index < len(row):
                        row_data['description'] = row[col_index] if row[col_index] else ''
                
                if 'company_name' in column_mapping:
                    col_index = ord(column_mapping['company_name']) - ord('A')
                    if col_index < len(row):
                        row_data['company_name'] = row[col_index] if row[col_index] else ''
                
                if 'website' in column_mapping:
                    col_index = ord(column_mapping['website']) - ord('A')
                    if col_index < len(row):
                        row_data['website'] = row[col_index] if row[col_index] else ''
                
                mapped_data.append(row_data)
            
            return pd.DataFrame(mapped_data)
            
        except HttpError as e:
            st.error(f"❌ Google Sheets API error: {e}")
            return None
        except Exception as e:
            st.error(f"❌ Error fetching sheet data: {e}")
            return None
    
    def update_row_results(self, sheet_id: str, row_num: int, category: str, brand_name: str, 
                          email_question: str, enriched_columns: Dict[str, str], sheet_name: str = "Sheet1"):
        """Update result columns for a specific row - always in new columns"""
        try:
            if not self.service:
                return False
            
            # Update enriched data in the designated columns
            range_name = f"{sheet_name}!{enriched_columns['category']}{row_num}:{enriched_columns['status']}{row_num}"
            
            body = {
                'values': [[category, brand_name, email_question, "✅ Complete"]]
            }
            
            self.service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            return True
            
        except Exception as e:
            print(f"Error updating results: {e}")
            return False
    
    def update_row_status(self, sheet_id: str, row_num: int, status: str, 
                         enriched_columns: Dict[str, str], sheet_name: str = "Sheet1"):
        """Update status column for a specific row"""
        try:
            if not self.service:
                return False
            
            # Update only the status column
            range_name = f"{sheet_name}!{enriched_columns['status']}{row_num}"
            
            body = {
                'values': [[status]]
            }
            
            self.service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            return True
            
        except Exception as e:
            print(f"Error updating status: {e}")
            return False
    
    def update_row_error(self, sheet_id: str, row_num: int, error_msg: str, 
                        enriched_columns: Dict[str, str], sheet_name: str = "Sheet1"):
        """Update row with error status"""
        try:
            if not self.service:
                return False
            
            # Update status column with error
            range_name = f"{sheet_name}!{enriched_columns['status']}{row_num}"
            
            body = {
                'values': [[f"❌ Error: {error_msg[:500]}..."]]
            }
            
            self.service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            return True
            
        except Exception as e:
            print(f"Error updating error status: {e}")
            return False
    
    def process_sheet_range(self, sheet_id: str, start_row: int, num_rows: int, 
                           progress_callback=None, sheet_name: str = "Sheet1", processing_mode: str = None) -> Dict:
        """
        Process a range of rows in Google Sheets with real-time updates
        Headers are always detected from row 1, data starts from row 2 or specified start_row
        
        Args:
            sheet_id: Google Sheets ID
            start_row: Starting row number for data (minimum 2)
            num_rows: Number of rows to process
            progress_callback: Function to call for progress updates
            sheet_name: Name of the sheet tab
            
        Returns:
            Dict with processing results
        """
        if not self.service:
            return {"error": "Not authenticated with Google Sheets"}
        
        try:
            # Step 1: Detect headers and column structure
            header_info = self.detect_headers(sheet_id, sheet_name)
            if not header_info:
                return {"error": "Failed to detect headers"}
            
            column_mapping = header_info['column_mapping']
            enriched_columns = header_info['enriched_columns']
            
            # Use manual processing mode or auto-detect
            if processing_mode == "CASE_A":
                case_type = "CASE_A"
            elif processing_mode == "CASE_B":
                case_type = "CASE_B"
            else:
                # Auto-detect (fallback)
                case_type = self._detect_processing_case(column_mapping)
            
            # Validate required columns based on case type
            missing_cols = []
            if case_type == "CASE_A":
                if 'keywords' not in column_mapping:
                    missing_cols.append('Company Keywords')
                if 'description' not in column_mapping:
                    missing_cols.append('Company Short Description')
            elif case_type == "CASE_B":
                if 'website' not in column_mapping:
                    missing_cols.append('Website')
                # company_name is optional for Case B
            else:
                return {"error": "Could not detect valid data format. Please ensure your sheet has either:\n- Case A: 'Company Keywords' + 'Company Short Description'\n- Case B: 'Website' + 'Company Name' (optional)"}
            
            if missing_cols:
                required_format = "Case A: keywords + description" if case_type == "CASE_A" else "Case B: website + company name"
                return {"error": f"Missing required columns for {required_format}: {', '.join(missing_cols)}"}
            
            # Step 2: Setup enriched data headers
            existing_enriched = header_info.get('existing_enriched', False)
            self.setup_enriched_headers(sheet_id, enriched_columns, existing_enriched, sheet_name)
            
            # Step 3: Get data from sheets
            df = self.get_sheet_data(sheet_id, start_row, num_rows, column_mapping, sheet_name)
            if df is None:
                return {"error": "Failed to fetch data from Google Sheets"}
            
            # Step 4: Process each row
            processed_count = 0
            success_count = 0
            error_count = 0
            skipped_rows = []
            
            start_time = time.time()
            
            for idx, row in df.iterrows():
                # Check for pause/stop signals
                if st.session_state.get('processing_paused', False):
                    st.warning("⏸️ Processing paused by user")
                    return {
                        "success": True,
                        "processed_count": processed_count,
                        "success_count": success_count,
                        "error_count": error_count,
                        "skipped_rows": skipped_rows,
                        "total_time": time.time() - start_time,
                        "avg_time_per_row": (time.time() - start_time) / processed_count if processed_count > 0 else 0,
                        "column_mapping": column_mapping,
                        "enriched_columns": enriched_columns,
                        "status": "paused"
                    }
                
                if st.session_state.get('processing_stopped', False):
                    st.error("⏹️ Processing stopped by user")
                    return {
                        "success": True,
                        "processed_count": processed_count,
                        "success_count": success_count,
                        "error_count": error_count,
                        "skipped_rows": skipped_rows,
                        "total_time": time.time() - start_time,
                        "avg_time_per_row": (time.time() - start_time) / processed_count if processed_count > 0 else 0,
                        "column_mapping": column_mapping,
                        "enriched_columns": enriched_columns,
                        "status": "stopped"
                    }
                
                actual_row_num = row['row_number']
                
                try:
                    # Update status to processing
                    self.update_row_status(sheet_id, actual_row_num, "⏳ Processing...", enriched_columns, sheet_name)
                    
                    # Process based on case type
                    if case_type == "CASE_A":
                        # Case A: Keywords + Description processing
                        keywords = self._clean_text(str(row.get('keywords', '')))
                        description = self._clean_text(str(row.get('description', '')))
                        company_context = self._clean_text(str(row.get('company_name', '')))
                        
                        # Skip empty rows
                        if not keywords and not description:
                            self.update_row_status(sheet_id, actual_row_num, "⏭️ Skipped (empty)", enriched_columns, sheet_name)
                            skipped_rows.append(actual_row_num)
                            processed_count += 1
                            continue
                        
                        # Process with OpenAI directly
                        result = self.categorizer.categorize_and_extract_brand(keywords, description, company_context)
                        
                    elif case_type == "CASE_B":
                        # Case B: Website + Company processing (with scrapy)
                        website = self._clean_text(str(row.get('website', '')))
                        company_name = self._clean_text(str(row.get('company_name', '')))
                        
                        # Skip empty rows
                        if not website:
                            self.update_row_status(sheet_id, actual_row_num, "⏭️ Skipped (no website)", enriched_columns, sheet_name)
                            skipped_rows.append(actual_row_num)
                            processed_count += 1
                            continue
                        
                        # Process with scrapy + OpenAI
                        result = self._process_case_b_row(website, company_name, actual_row_num, enriched_columns, sheet_id, sheet_name)
                    
                    # Update results in sheet (same row, new columns)
                    self.update_row_results(
                        sheet_id, actual_row_num,
                        result['category'],
                        result['brand_name'],
                        result['email_question'],
                        enriched_columns,
                        sheet_name
                    )
                    
                    success_count += 1
                    processed_count += 1
                    
                    # Calculate progress and ETA
                    elapsed_time = time.time() - start_time
                    progress_percentage = (processed_count / num_rows) * 100
                    
                    if processed_count > 0:
                        avg_time_per_row = elapsed_time / processed_count
                        remaining_rows = num_rows - processed_count
                        eta_seconds = remaining_rows * avg_time_per_row
                        eta_minutes = eta_seconds / 60
                    else:
                        eta_minutes = 0
                    
                    # Update progress
                    if progress_callback:
                        progress_callback(
                            progress_percentage,
                            f"Row {actual_row_num}: {result['category']} | ETA: {eta_minutes:.1f}m"
                        )
                    
                    # Rate limiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    # Handle row error
                    error_msg = str(e)[:50]
                    self.update_row_error(sheet_id, actual_row_num, error_msg, enriched_columns, sheet_name)
                    error_count += 1
                    processed_count += 1
                    skipped_rows.append(actual_row_num)
                    
                    print(f"❌ Error processing row {actual_row_num}: {e}")
            
            # Final results
            total_time = time.time() - start_time
            
            return {
                "success": True,
                "processed_count": processed_count,
                "success_count": success_count,
                "error_count": error_count,
                "skipped_rows": skipped_rows,
                "total_time": total_time,
                "avg_time_per_row": total_time / processed_count if processed_count > 0 else 0,
                "column_mapping": column_mapping,
                "enriched_columns": enriched_columns
            }
            
        except Exception as e:
            return {"error": f"Processing failed: {str(e)}"}
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if pd.isna(text) or text == 'nan':
            return ""
        
        text = str(text).strip()
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = ' '.join(text.split())
        
        return text
