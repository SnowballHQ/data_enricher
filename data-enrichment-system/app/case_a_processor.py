import pandas as pd
import logging
import sys
import os
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to sys.path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.openai_categorizer import OpenAICategorizer
try:
    from config.settings import CHUNK_SIZE, OPENAI_API_KEY
except ImportError:
    # Fallback values if config not found
    CHUNK_SIZE = 1000
    OPENAI_API_KEY = None

class CaseAProcessor:
    """Processor for Case A: Files with keywords and description columns"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        if not OPENAI_API_KEY:
            raise ValueError("OpenAI API key is required for categorization")
        self.categorizer = OpenAICategorizer(OPENAI_API_KEY)
    
    def process_dataframe(self, df: pd.DataFrame, progress_callback=None) -> pd.DataFrame:
        """Process DataFrame with keywords and description columns"""
        try:
            # Identify required columns
            column_mapping = self._identify_columns(df.columns)
            
            if not all(column_mapping.values()):
                missing = [k for k, v in column_mapping.items() if not v]
                raise ValueError(f"Missing required columns: {missing}")
            
            # Initialize new columns
            df['category'] = ''
            df['brand_name'] = ''
            df['processing_status'] = 'pending'
            
            total_rows = len(df)
            processed_rows = 0
            
            self.logger.info(f"Starting Case A processing for {total_rows} rows")
            
            # Process in chunks for better performance
            chunk_size = min(CHUNK_SIZE, total_rows)
            chunks = [df[i:i+chunk_size] for i in range(0, total_rows, chunk_size)]
            
            for chunk_idx, chunk in enumerate(chunks):
                self.logger.info(f"Processing chunk {chunk_idx + 1}/{len(chunks)}")
                
                # Process chunk
                processed_chunk = self._process_chunk(chunk, column_mapping)
                
                # Update main DataFrame
                df.iloc[chunk.index] = processed_chunk
                
                processed_rows += len(chunk)
                
                # Update progress
                if progress_callback:
                    progress = (processed_rows / total_rows) * 100
                    progress_callback(progress, f"Processed {processed_rows}/{total_rows} rows")
            
            success_count = len(df[df['processing_status'] == 'success'])
            error_count = len(df[df['processing_status'] == 'error'])
            
            self.logger.info(f"Case A processing completed: {success_count} successful, {error_count} errors")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error in Case A processing: {e}")
            raise
    
    def _identify_columns(self, columns: List[str]) -> Dict[str, str]:
        """Identify required columns in the DataFrame"""
        columns_lower = [col.lower().strip() for col in columns]
        mapping = {
            'keywords': None,
            'description': None,
            'company_name': None
        }
        
        for i, col in enumerate(columns_lower):
            original_col = columns[i]
            
            if 'keyword' in col and not mapping['keywords']:
                mapping['keywords'] = original_col
            elif ('description' in col or 'desc' in col) and not mapping['description']:
                mapping['description'] = original_col
            elif ('company' in col or 'name' in col or 'business' in col) and not mapping['company_name']:
                mapping['company_name'] = original_col
        
        self.logger.info(f"Column mapping: {mapping}")
        return mapping
    
    def _process_chunk(self, chunk: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """Process a chunk of data"""
        for idx, row in chunk.iterrows():
            try:
                # Extract data from mapped columns
                keywords = str(row[column_mapping['keywords']]) if pd.notna(row[column_mapping['keywords']]) else ""
                description = str(row[column_mapping['description']]) if pd.notna(row[column_mapping['description']]) else ""
                company_name = str(row[column_mapping['company_name']]) if pd.notna(row[column_mapping['company_name']]) else ""
                
                # Combine keywords and description
                combined_text = f"{keywords} {description}".strip()
                
                if not combined_text:
                    chunk.at[idx, 'category'] = 'Unknown'
                    chunk.at[idx, 'brand_name'] = 'No content available for brand extraction'
                    chunk.at[idx, 'processing_status'] = 'error'
                    continue
                
                # Extract category and brand name using OpenAI
                result = self.categorizer.categorize_and_extract_brand("", combined_text, company_name)
                category = result['category']
                brand_name = result['brand_name']
                
                # Update row
                chunk.at[idx, 'category'] = category
                chunk.at[idx, 'brand_name'] = brand_name
                chunk.at[idx, 'processing_status'] = 'success'
                
            except Exception as e:
                self.logger.error(f"Error processing row {idx}: {e}")
                chunk.at[idx, 'category'] = 'Unknown'
                chunk.at[idx, 'brand_name'] = 'Error extracting brand name'
                chunk.at[idx, 'processing_status'] = 'error'
        
        return chunk
    
    def _process_row_parallel(self, row_data: Tuple[int, pd.Series], column_mapping: Dict[str, str]) -> Tuple[int, Dict]:
        """Process single row (for parallel processing)"""
        idx, row = row_data
        result = {
            'category': 'Unknown',
            'brand_name': 'Error extracting brand name',
            'processing_status': 'error'
        }
        
        try:
            # Extract data
            keywords = str(row[column_mapping['keywords']]) if pd.notna(row[column_mapping['keywords']]) else ""
            description = str(row[column_mapping['description']]) if pd.notna(row[column_mapping['description']]) else ""
            company_name = str(row[column_mapping['company_name']]) if pd.notna(row[column_mapping['company_name']]) else ""
            
            combined_text = f"{keywords} {description}".strip()
            
            if combined_text:
                # Extract category and brand name using OpenAI
                api_result = self.categorizer.categorize_and_extract_brand("", combined_text, company_name)
                category = api_result['category']
                brand_name = api_result['brand_name']
                
                result = {
                    'category': category,
                    'brand_name': brand_name,
                    'processing_status': 'success'
                }
            
        except Exception as e:
            self.logger.error(f"Error in parallel processing row {idx}: {e}")
        
        return idx, result
    
    def process_dataframe_parallel(self, df: pd.DataFrame, max_workers: int = 4, progress_callback=None) -> pd.DataFrame:
        """Process DataFrame using parallel processing"""
        try:
            column_mapping = self._identify_columns(df.columns)
            
            if not all(column_mapping.values()):
                missing = [k for k, v in column_mapping.items() if not v]
                raise ValueError(f"Missing required columns: {missing}")
            
            # Initialize columns
            df['category'] = ''
            df['brand_name'] = ''
            df['processing_status'] = 'pending'
            
            total_rows = len(df)
            processed_rows = 0
            
            self.logger.info(f"Starting parallel Case A processing for {total_rows} rows with {max_workers} workers")
            
            # Process with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_idx = {
                    executor.submit(self._process_row_parallel, (idx, row), column_mapping): idx 
                    for idx, row in df.iterrows()
                }
                
                # Collect results
                for future in as_completed(future_to_idx):
                    try:
                        idx, result = future.result()
                        
                        # Update DataFrame
                        df.at[idx, 'category'] = result['category']
                        df.at[idx, 'brand_name'] = result['brand_name']
                        df.at[idx, 'processing_status'] = result['processing_status']
                        
                        processed_rows += 1
                        
                        # Update progress
                        if progress_callback and processed_rows % 10 == 0:  # Update every 10 rows
                            progress = (processed_rows / total_rows) * 100
                            progress_callback(progress, f"Processed {processed_rows}/{total_rows} rows")
                        
                    except Exception as e:
                        self.logger.error(f"Error collecting result: {e}")
            
            success_count = len(df[df['processing_status'] == 'success'])
            error_count = len(df[df['processing_status'] == 'error'])
            
            self.logger.info(f"Parallel Case A processing completed: {success_count} successful, {error_count} errors")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error in parallel Case A processing: {e}")
            raise