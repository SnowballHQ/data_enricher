"""
Google Sheets Integration for Real-time Data Processing
"""

import os
import json
import time
import re
from typing import Optional, Dict, List, Tuple
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
from utils.openai_categorizer import OpenAICategorizer

class GoogleSheetsProcessor:
    """Google Sheets processor with OAuth authentication and real-time updates"""
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    def __init__(self, api_key: str):
        """Initialize with OpenAI API key"""
        self.categorizer = OpenAICategorizer(api_key)
        self.service = None
        self.credentials = None
        
    def authenticate_oauth(self, credentials_json: dict) -> bool:
        """
        Authenticate with Google Sheets using OAuth
        
        Args:
            credentials_json: OAuth2 client credentials
            
        Returns:
            bool: True if authentication successful
        """
        try:
            # Check if we have stored credentials
            if 'google_sheets_creds' in st.session_state:
                self.credentials = Credentials.from_authorized_user_info(
                    st.session_state.google_sheets_creds, self.SCOPES
                )
                
            # If no valid credentials, initiate OAuth flow
            if not self.credentials or not self.credentials.valid:
                if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                    try:
                        self.credentials.refresh(Request())
                    except Exception as e:
                        print(f"Failed to refresh credentials: {e}")
                        self.credentials = None
                
                if not self.credentials:
                    # Create OAuth flow
                    flow = Flow.from_client_config(
                        credentials_json,
                        scopes=self.SCOPES,
                        redirect_uri='urn:ietf:wg:oauth:2.0:oob'
                    )
                    
                    # Get authorization URL
                    auth_url, _ = flow.authorization_url(prompt='consent')
                    
                    st.warning("⚠️ Please authorize access to Google Sheets:")
                    st.markdown(f"1. [Click here to authorize]({auth_url})")
                    st.markdown("2. Copy the authorization code and paste it below:")
                    
                    auth_code = st.text_input("Authorization Code:", type="password")
                    
                    if auth_code:
                        try:
                            # Exchange code for credentials
                            flow.fetch_token(code=auth_code)
                            self.credentials = flow.credentials
                            
                            # Store credentials in session
                            st.session_state.google_sheets_creds = {
                                'token': self.credentials.token,
                                'refresh_token': self.credentials.refresh_token,
                                'token_uri': self.credentials.token_uri,
                                'client_id': self.credentials.client_id,
                                'client_secret': self.credentials.client_secret,
                                'scopes': self.credentials.scopes
                            }
                            
                            st.success("✅ Authentication successful!")
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"❌ Authentication failed: {e}")
                            return False
                    else:
                        return False
            
            # Build service
            self.service = build('sheets', 'v4', credentials=self.credentials)
            return True
            
        except Exception as e:
            st.error(f"❌ Authentication error: {e}")
            return False
    
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
    
    def get_sheet_data(self, sheet_id: str, start_row: int, num_rows: int, sheet_name: str = "Sheet1") -> Optional[pd.DataFrame]:
        """
        Get data from Google Sheets
        
        Args:
            sheet_id: Google Sheets ID
            start_row: Starting row number (1-based)
            num_rows: Number of rows to fetch
            sheet_name: Name of the sheet tab
            
        Returns:
            DataFrame with the data
        """
        try:
            if not self.service:
                st.error("❌ Not authenticated with Google Sheets")
                return None
            
            # Calculate range
            end_row = start_row + num_rows - 1
            range_name = f"{sheet_name}!A{start_row}:Z{end_row}"
            
            # Get values
            result = self.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                st.warning("⚠️ No data found in the specified range")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(values)
            
            # Use first row as headers if starting from row 1
            if start_row == 1 and len(df) > 0:
                df.columns = df.iloc[0]
                df = df[1:].reset_index(drop=True)
            
            return df
            
        except HttpError as e:
            st.error(f"❌ Google Sheets API error: {e}")
            return None
        except Exception as e:
            st.error(f"❌ Error fetching sheet data: {e}")
            return None
    
    def update_row_status(self, sheet_id: str, row_num: int, status: str, sheet_name: str = "Sheet1"):
        """Update status column for a specific row"""
        try:
            if not self.service:
                return False
            
            # Assume status column is the last column (column G for now)
            range_name = f"{sheet_name}!G{row_num}"
            
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
    
    def update_row_results(self, sheet_id: str, row_num: int, category: str, brand_name: str, 
                          email_question: str, sheet_name: str = "Sheet1"):
        """Update result columns for a specific row"""
        try:
            if not self.service:
                return False
            
            # Assume columns D=category, E=brand_name, F=email_question, G=status
            range_name = f"{sheet_name}!D{row_num}:G{row_num}"
            
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
    
    def update_row_error(self, sheet_id: str, row_num: int, error_msg: str, sheet_name: str = "Sheet1"):
        """Update row with error status"""
        try:
            if not self.service:
                return False
            
            # Update status column with error
            range_name = f"{sheet_name}!G{row_num}"
            
            body = {
                'values': [[f"❌ Error: {error_msg[:50]}..."]]
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
                           progress_callback=None, sheet_name: str = "Sheet1") -> Dict:
        """
        Process a range of rows in Google Sheets with real-time updates
        
        Args:
            sheet_id: Google Sheets ID
            start_row: Starting row number
            num_rows: Number of rows to process
            progress_callback: Function to call for progress updates
            sheet_name: Name of the sheet tab
            
        Returns:
            Dict with processing results
        """
        if not self.service:
            return {"error": "Not authenticated with Google Sheets"}
        
        try:
            # Get data from sheets
            df = self.get_sheet_data(sheet_id, start_row, num_rows, sheet_name)
            if df is None:
                return {"error": "Failed to fetch data from Google Sheets"}
            
            # Map columns (assume standard format for now)
            df_mapped = self._map_columns(df.copy())
            
            # Initialize counters
            processed_count = 0
            success_count = 0
            error_count = 0
            skipped_rows = []
            
            start_time = time.time()
            
            # Process each row
            for idx, row in df_mapped.iterrows():
                actual_row_num = start_row + idx
                
                try:
                    # Update status to processing
                    self.update_row_status(sheet_id, actual_row_num, "⏳ Processing...", sheet_name)
                    
                    # Extract data
                    keywords = self._clean_text(str(row.get('keywords', '')))
                    description = self._clean_text(str(row.get('description', '')))
                    company_context = self._clean_text(str(row.get('company_name', '')))
                    
                    # Skip empty rows
                    if not keywords and not description:
                        self.update_row_status(sheet_id, actual_row_num, "⏭️ Skipped (empty)", sheet_name)
                        skipped_rows.append(actual_row_num)
                        processed_count += 1
                        continue
                    
                    # Process with OpenAI
                    result = self.categorizer.categorize_and_extract_brand(keywords, description, company_context)
                    
                    # Update results in sheet
                    self.update_row_results(
                        sheet_id, actual_row_num,
                        result['category'],
                        result['brand_name'],
                        result['email_question'],
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
                    
                    # Rate limiting - respect Google Sheets API limits
                    time.sleep(0.1)  # Small delay between requests
                    
                except Exception as e:
                    # Handle row error
                    error_msg = str(e)[:50]
                    self.update_row_error(sheet_id, actual_row_num, error_msg, sheet_name)
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
                "avg_time_per_row": total_time / processed_count if processed_count > 0 else 0
            }
            
        except Exception as e:
            return {"error": f"Processing failed: {str(e)}"}
    
    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map various column names to standard format"""
        df_mapped = df.copy()
        
        # Map keywords columns
        keywords_cols = ['Company Keywords', 'keywords', 'Keywords', 'tags', 'Tags']
        for col in keywords_cols:
            if col in df_mapped.columns:
                df_mapped['keywords'] = df_mapped[col]
                break
        
        # Map description columns
        desc_cols = ['Company Short Description', 'description', 'Description', 'about', 'About']
        for col in desc_cols:
            if col in df_mapped.columns:
                df_mapped['description'] = df_mapped[col]
                break
        
        # Map company name columns
        company_cols = ['Company Name', 'company_name', 'name', 'Name', 'brand', 'Brand']
        for col in company_cols:
            if col in df_mapped.columns:
                df_mapped['company_name'] = df_mapped[col]
                break
        
        return df_mapped
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if pd.isna(text) or text == 'nan':
            return ""
        
        text = str(text).strip()
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = ' '.join(text.split())
        
        return text
