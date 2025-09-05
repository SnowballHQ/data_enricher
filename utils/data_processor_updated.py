"""
Data Processing utilities for enriching company data
"""

import pandas as pd
import numpy as np
import os
import tempfile
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional
from utils.openai_categorizer import OpenAICategorizer
from utils.email_generator import EmailGenerator

class DataProcessor:
    """Main data processor for company data enrichment"""
    
    def __init__(self, api_key: str):
        """Initialize the data processor with required components"""
        self.categorizer = OpenAICategorizer(api_key)
        self.email_generator = EmailGenerator()
    
    def process_file(self, file_data: bytes, filename: str, instantly_date: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """
        Process uploaded file data and return enriched DataFrame
        
        Args:
            file_data: Raw file data in bytes
            filename: Original filename for format detection
            instantly_date: Optional date filter for 'Instantly Date' column
            
        Returns:
            Tuple of (enriched_dataframe, error_message)
        """
        try:
            # Read file into DataFrame
            df = self._read_file_data(file_data, filename)
            if df is None:
                return None, "Failed to read file data"
            
            print(f"✅ Successfully loaded {len(df)} rows from file")
            
            # Process the data
            enriched_df = self.process_dataframe(df, instantly_date)
            
            return enriched_df, None
            
        except Exception as e:
            error_msg = f"Error processing file: {str(e)}"
            print(f"❌ {error_msg}")
            return None, error_msg
    
    def _read_file_data(self, file_data: bytes, filename: str) -> Optional[pd.DataFrame]:
        """Read file data into DataFrame based on file extension"""
        try:
            file_extension = filename.lower().split('.')[-1]
            
            if file_extension == 'csv':
                # Try different encodings for CSV
                for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                    try:
                        df = pd.read_csv(io.BytesIO(file_data), encoding=encoding)
                        print(f"✅ CSV read successfully with {encoding} encoding")
                        return df
                    except UnicodeDecodeError:
                        continue
                print("❌ Failed to read CSV with any encoding")
                return None
                
            elif file_extension in ['xlsx', 'xls']:
                df = pd.read_excel(io.BytesIO(file_data))
                print(f"✅ Excel file read successfully")
                return df
            else:
                print(f"❌ Unsupported file format: {file_extension}")
                return None
                
        except Exception as e:
            print(f"❌ Error reading file data: {e}")
            return None
    
    def process_dataframe(self, df: pd.DataFrame, instantly_date: Optional[str] = None) -> pd.DataFrame:
        """
        Process a DataFrame and add category, brand name, and email question columns
        
        Args:
            df: Input DataFrame
            instantly_date: Optional date filter
            
        Returns:
            Enriched DataFrame with new columns
        """
        try:
            print(f"🔄 Starting processing of {len(df)} rows")
            
            # Map columns to standard names
            enriched_df = self._map_columns(df.copy())
            
            # Filter by Instantly Date if provided
            if instantly_date and 'Instantly Date' in enriched_df.columns:
                original_count = len(enriched_df)
                enriched_df = enriched_df[enriched_df['Instantly Date'] == instantly_date].copy()
                filtered_count = len(enriched_df)
                print(f"📅 Filtered by Instantly Date '{instantly_date}': {original_count} → {filtered_count} rows")
                
                if filtered_count == 0:
                    print(f"❌ No rows found with Instantly Date = '{instantly_date}'")
                    return enriched_df
            
            # Add category, brand_name, and email_question columns
            categories, brand_names, email_questions = self._categorize_and_extract_brands(enriched_df)
            enriched_df['category'] = categories
            enriched_df['brand_name'] = brand_names
            enriched_df['email_question'] = email_questions
            
            return enriched_df
            
        except Exception as e:
            print(f"❌ Error in process_dataframe: {e}")
            raise
    
    def _categorize_and_extract_brands(self, df: pd.DataFrame) -> Tuple[list, list, list]:
        """Categorize all products and extract brand names and email questions using concurrent processing"""
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
        email_questions = [None] * total_rows
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
                    email_questions[index] = result['email_question']
                    completed_count += 1
                    print(f"✅ Completed {completed_count}/{total_rows} - Row {index + 1}: {result['category']} | {result['brand_name']} | {result['email_question']}")
                except Exception as e:
                    failed_rows.append((index + 1, str(e)))
                    print(f"❌ Error processing row {index + 1}: {str(e)}")
        
        # Handle failed rows
        if failed_rows:
            print(f"⚠️ {len(failed_rows)} rows failed processing:")
            for row_num, error in failed_rows[:5]:  # Show first 5 errors
                print(f"   Row {row_num}: {error}")
            if len(failed_rows) > 5:
                print(f"   ... and {len(failed_rows) - 5} more")
        
        # Replace None values with fallbacks for failed rows
        for i in range(total_rows):
            if categories[i] is None:
                categories[i] = "Unknown Category"
            if brand_names[i] is None:
                brand_names[i] = "Unknown Brand"
            if email_questions[i] is None:
                email_questions[i] = "What are the best local brands?"
        
        print(f"✅ Completed categorization and brand extraction: {completed_count}/{total_rows} successful")
        return categories, brand_names, email_questions
    
    def _categorize_and_extract_single(self, row_data: dict) -> dict:
        """Process a single row for categorization and brand extraction"""
        keywords = row_data['keywords']
        description = row_data['description']
        company_context = row_data['company_context']
        index = row_data['index']
        
        print(f"🔍 DEBUG - Processing row {index + 1}")
        print(f"🔍 DEBUG - Row {index + 1} keywords: {keywords[:50]}...")
        print(f"🔍 DEBUG - Row {index + 1} description: {description[:50]}...")
        
        # Get category, brand name, and email question from OpenAI - let exceptions propagate up
        result = self.categorizer.categorize_and_extract_brand(keywords, description, company_context)
        return result
    
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text for better categorization"""
        if pd.isna(text):
            return ""
        
        # Convert to string and clean
        text = str(text).strip()
        
        # Remove common artifacts
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = ' '.join(text.split())  # Normalize whitespace
        
        return text
    
    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map various column names to standard format for processing"""
        print(f"🔍 DEBUG - Input DataFrame columns: {list(df.columns)}")
        
        # Create a copy to avoid modifying the original
        df_mapped = df.copy()
        
        # Map keywords columns - prioritize exact matches first
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
            'Company Short Description',
            'description', 'Description',
            'company description', 'Company Description',
            'product description', 'Product Description',
            'about', 'About'
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
        company_columns = [
            'Company Name',
            'company_name', 'company name',
            'name', 'Name',
            'brand', 'Brand',
            'organization', 'Organization'
        ]
        company_found = False
        for col in company_columns:
            if col in df_mapped.columns:
                df_mapped['company_name'] = df_mapped[col]
                company_found = True
                print(f"✅ Mapped '{col}' to 'company_name'")
                break
        
        if not company_found:
            print("⚠️ No company name column found - will use empty string")
            df_mapped['company_name'] = ""
        
        print(f"🔍 DEBUG - Final mapped columns: keywords={keywords_found}, description={description_found}, company_name={company_found}")
        
        return df_mapped
    
    def export_to_excel(self, df: pd.DataFrame) -> bytes:
        """Export DataFrame to Excel format and return as bytes"""
        try:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Enriched Data', index=False)
            output.seek(0)
            return output.read()
        except Exception as e:
            print(f"❌ Error exporting to Excel: {e}")
            raise
    
    def export_to_csv(self, df: pd.DataFrame) -> str:
        """Export DataFrame to CSV format and return as string"""
        try:
            return df.to_csv(index=False)
        except Exception as e:
            print(f"❌ Error exporting to CSV: {e}")
            raise
