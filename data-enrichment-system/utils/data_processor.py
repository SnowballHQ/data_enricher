"""
Data processing module for handling Excel/CSV files
"""

import pandas as pd
import io
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, Optional, Dict
from config import SUPPORTED_FORMATS, EMAIL_TEMPLATE
from utils.openai_categorizer import OpenAICategorizer

class DataProcessor:
    def __init__(self, openai_api_key: str):
        """Initialize the data processor with OpenAI API key"""
        self.categorizer = OpenAICategorizer(openai_api_key)
    
    def process_file(self, file_content: bytes, filename: str, instantly_date: str = None) -> Tuple[pd.DataFrame, str]:
        """
        Process uploaded file and return enriched DataFrame
        
        Args:
            file_content: File content as bytes
            filename: Name of the uploaded file
            instantly_date: Optional date filter for 'Instantly Date' column (e.g., '28/08')
            
        Returns:
            Tuple of (enriched DataFrame, error message if any)
        """
        try:
            # Read the file based on its format
            df = self._read_file(file_content, filename)
            
            # Map column names to standard names
            df = self._map_column_names(df)
            
            # Validate required columns
            error_msg = self._validate_columns(df)
            if error_msg:
                return None, error_msg
            
            # Process the data
            enriched_df = self._enrich_data(df, instantly_date)
            
            return enriched_df, None
            
        except Exception as e:
            return None, f"Error processing file: {str(e)}"
    
    def _read_file(self, file_content: bytes, filename: str) -> pd.DataFrame:
        """Read file content based on file extension"""
        file_extension = filename.lower()
        
        print(f"🔍 DEBUG - Reading file: {filename}")
        print(f"🔍 DEBUG - File size: {len(file_content)} bytes")
        print(f"🔍 DEBUG - File extension: {file_extension}")
        
        try:
            if file_extension.endswith('.csv'):
                print("🔍 DEBUG - Attempting to read as CSV...")
                # Try different encodings and separators for CSV files
                try:
                    df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8')
                    print(f"✅ Successfully read CSV with UTF-8 encoding")
                except UnicodeDecodeError:
                    print("⚠️ UTF-8 failed, trying Latin-1...")
                    df = pd.read_csv(io.BytesIO(file_content), encoding='latin-1')
                    print(f"✅ Successfully read CSV with Latin-1 encoding")
                except pd.errors.EmptyDataError:
                    print("❌ CSV file appears to be empty")
                    raise ValueError("The uploaded CSV file is empty")
                except Exception as e:
                    print(f"⚠️ Standard CSV read failed: {e}")
                    # Try with different separator
                    try:
                        df = pd.read_csv(io.BytesIO(file_content), sep=';', encoding='utf-8')
                        print(f"✅ Successfully read CSV with semicolon separator")
                    except:
                        df = pd.read_csv(io.BytesIO(file_content), sep='\t', encoding='utf-8')
                        print(f"✅ Successfully read CSV with tab separator")
                        
            elif file_extension.endswith(('.xlsx', '.xls')):
                print("🔍 DEBUG - Attempting to read as Excel...")
                df = pd.read_excel(io.BytesIO(file_content))
                print(f"✅ Successfully read Excel file")
            else:
                raise ValueError(f"Unsupported file format. Supported formats: {SUPPORTED_FORMATS}")
            
            print(f"🔍 DEBUG - DataFrame shape after reading: {df.shape}")
            print(f"🔍 DEBUG - DataFrame columns: {list(df.columns)}")
            print(f"🔍 DEBUG - First few rows preview:")
            print(df.head().to_string())
            
            if df.empty:
                raise ValueError("The file was read successfully but contains no data")
            
            return df
            
        except Exception as e:
            print(f"❌ Error reading file {filename}: {str(e)}")
            raise
    
    def _map_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map various column names to standard names for processing"""
        df_mapped = df.copy()
        
        # Debug: Show original columns
        print(f"🔍 DEBUG - Original columns in DataFrame: {list(df_mapped.columns)}")
        print(f"🔍 DEBUG - DataFrame shape: {df_mapped.shape}")
        
        # Map keywords columns - prioritize exact matches
        keywords_columns = ['Company Keywords']
        keywords_found = False
        for col in keywords_columns:
            if col in df_mapped.columns:
                df_mapped['keywords'] = df_mapped[col]
                keywords_found = True
                print(f"✅ Mapped '{col}' to 'keywords'")
                break
        
        
        if not keywords_found:
            print("❌ No keywords column found in mapping")
        
        # Map description columns - prioritize exact matches
        description_columns = [
            'Company Short Description'
        ]
        description_found = False
        for col in description_columns:
            if col in df_mapped.columns:
                df_mapped['description'] = df_mapped[col]
                description_found = True
                print(f"✅ Mapped '{col}' to 'description'")
                break
        
        
        if not description_found:
            print("❌ No description column found in mapping")
        
        # Map company name columns - prioritize exact matches
        company_columns = ['Company Name']
        company_found = False
        for col in company_columns:
            if col in df_mapped.columns:
                df_mapped['company_name'] = df_mapped[col]
                company_found = True
                print(f"✅ Mapped '{col}' to 'company_name'")
                break
        
        
        if not company_found:
            print("ℹ️ No company column found in mapping (will use placeholder)")
        
        # Debug: Show final mapping results
        print(f"🔍 DEBUG - Final columns in DataFrame: {list(df_mapped.columns)}")
        print(f"🔍 DEBUG - Required columns found: keywords={keywords_found}, description={description_found}")
        print(f"🔍 DEBUG - Optional company column found: {company_found}")
        
        return df_mapped
    
    def _validate_columns(self, df: pd.DataFrame) -> Optional[str]:
        """Validate that required columns exist after mapping"""
        required_columns = ['keywords', 'description']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return f"Missing required columns: {', '.join(missing_columns)}. Please ensure your file contains columns that can be mapped to 'keywords' and 'description'."
        
        return None
    
    def _enrich_data(self, df: pd.DataFrame, instantly_date: str = None) -> pd.DataFrame:
        """Enrich the DataFrame with categories and brand names"""
        # Create a copy to avoid modifying the original
        enriched_df = df.copy()
        
        # Filter for rows where 'Instantly Date' matches the specified date
        if instantly_date and 'Instantly Date' in enriched_df.columns:
            original_count = len(enriched_df)
            enriched_df = enriched_df[enriched_df['Instantly Date'] == instantly_date].copy()
            filtered_count = len(enriched_df)
            print(f"🔍 DEBUG - Filtered data: {original_count} total rows → {filtered_count} rows with Instantly Date = '{instantly_date}'")
        elif instantly_date:
            print(f"⚠️ DEBUG - 'Instantly Date' column not found, processing all rows (filter '{instantly_date}' ignored)")
        else:
            print("🔍 DEBUG - No date filter applied, processing all rows")
        
        if instantly_date and len(enriched_df) == 0:
            print(f"❌ No rows found with Instantly Date = '{instantly_date}'")
            return enriched_df
        
        # Add category and brand_name columns
        categories, brand_names = self._categorize_and_extract_brands(enriched_df)
        enriched_df['category'] = categories
        enriched_df['brand_name'] = brand_names
        
        return enriched_df
    
    def _categorize_and_extract_brands(self, df: pd.DataFrame) -> Tuple[list, list]:
        """Categorize all products and extract brand names using concurrent processing"""
        total_rows = len(df)
        print(f"🔍 DEBUG - Starting concurrent categorization and brand extraction for {total_rows} rows")
        print(f"🔍 DEBUG - DataFrame shape: {df.shape}")
        print(f"🔍 DEBUG - DataFrame index range: {df.index.min()} to {df.index.max()}")
        
        # Reset index to ensure consecutive numbering
        df_reset = df.reset_index(drop=True)
        total_rows = len(df_reset)
        print(f"🔍 DEBUG - After reset_index: {total_rows} rows")
        
        # Prepare data for concurrent processing
        rows_data = []
        for idx, row in df_reset.iterrows():
            keywords = self._clean_text(str(row.get('keywords', '')))
            description = self._clean_text(str(row.get('description', '')))
            company_context = self._clean_text(str(row.get('company_name', '')))
            rows_data.append({
                'index': idx,
                'keywords': keywords,
                'description': description,
                'company_context': company_context
            })
        
        # Use ThreadPoolExecutor for concurrent API calls
        max_workers = min(10, total_rows)  # Limit to 10 concurrent requests
        print(f"🔍 DEBUG - Using {max_workers} concurrent workers")
        
        # Initialize results lists with None values
        categories = [None] * total_rows
        brand_names = [None] * total_rows
        completed_count = 0
        failed_rows = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_index = {}
            for i, row_data in enumerate(rows_data):
                future = executor.submit(self._categorize_and_extract_single, row_data)
                future_to_index[future] = i
            
            # Process completed tasks
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    result = future.result()
                    categories[index] = result['category']
                    brand_names[index] = result['brand_name']
                    completed_count += 1
                    print(f"✅ Completed {completed_count}/{total_rows} - Row {index + 1}: {result['category']} | {result['brand_name']}")
                except Exception as e:
                    failed_rows.append((index + 1, str(e)))
                    print(f"❌ Error processing row {index + 1}: {str(e)}")
        
        # Check if any rows failed processing
        if failed_rows:
            error_msg = f"Failed to process {len(failed_rows)} rows:\n"
            for row_num, error in failed_rows:
                error_msg += f"Row {row_num}: {error}\n"
            raise RuntimeError(error_msg)
        
        print(f"🔍 DEBUG - Concurrent processing completed. Total items: {len(categories)}")
        return categories, brand_names
    
    def _categorize_and_extract_single(self, row_data: dict) -> Dict[str, str]:
        """Categorize and extract brand for a single product - used for concurrent processing"""
        keywords = row_data['keywords']
        description = row_data['description']
        company_context = row_data['company_context']
        index = row_data['index']
        
        print(f"🔍 DEBUG - Processing row {index + 1}")
        print(f"🔍 DEBUG - Row {index + 1} keywords: {keywords[:50]}...")
        print(f"🔍 DEBUG - Row {index + 1} description: {description[:50]}...")
        
        # Get category and brand name from OpenAI - let exceptions propagate up
        result = self.categorizer.categorize_and_extract_brand(keywords, description, company_context)
        return result
    
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text for better categorization"""
        if pd.isna(text):
            return ""
        
        # Convert to string and clean
        text = str(text).strip()
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text
    
    def _extract_company_name(self, row: pd.Series) -> str:
        """Extract company name from the row data"""
        # Try to find company name in various columns
        company_columns = ['company', 'company_name', 'name', 'brand', 'organization']
        
        for col in company_columns:
            if col in row and pd.notna(row[col]):
                return str(row[col]).strip()
        
        # If no company name found, use a generic placeholder
        return "your company"
    
    def export_to_excel(self, df: pd.DataFrame) -> bytes:
        """Export DataFrame to Excel format"""
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Enriched Data')
        
        output.seek(0)
        return output.getvalue()
    
    def export_to_csv(self, df: pd.DataFrame) -> str:
        """Export DataFrame to CSV format"""
        return df.to_csv(index=False)
