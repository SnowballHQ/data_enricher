import pandas as pd
import logging
import subprocess
import json
import os
import tempfile
import sys
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import validators

# Add parent directory to sys.path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.openai_categorizer import OpenAICategorizer
try:
    from config.settings import CHUNK_SIZE, OPENAI_API_KEY
except ImportError:
    # Fallback values if config not found
    CHUNK_SIZE = 1000
    OPENAI_API_KEY = None

class CaseBProcessor:
    """Processor for Case B: Files with website URLs requiring scraping"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        if not OPENAI_API_KEY:
            raise ValueError("OpenAI API key is required for categorization")
        self.categorizer = OpenAICategorizer(OPENAI_API_KEY)
        self.scraped_data_cache = {}
    
    def process_dataframe(self, df: pd.DataFrame, progress_callback=None) -> pd.DataFrame:
        """Process DataFrame with website URLs"""
        try:
            # Identify required columns
            column_mapping = self._identify_columns(df.columns)
            
            if not all(column_mapping.values()):
                missing = [k for k, v in column_mapping.items() if not v]
                raise ValueError(f"Missing required columns: {missing}")
            
            # Initialize new columns
            df['scraped_content'] = ''
            df['category'] = ''
            df['brand_name'] = ''
            df['processing_status'] = 'pending'
            df['scraping_status'] = 'pending'
            
            total_rows = len(df)
            processed_rows = 0
            
            self.logger.info(f"Starting Case B processing for {total_rows} rows")
            
            # Process in chunks
            chunk_size = min(CHUNK_SIZE, total_rows)
            chunks = [df[i:i+chunk_size] for i in range(0, total_rows, chunk_size)]
            
            for chunk_idx, chunk in enumerate(chunks):
                self.logger.info(f"Processing chunk {chunk_idx + 1}/{len(chunks)}")
                
                # Extract URLs from chunk
                urls = self._extract_urls_from_chunk(chunk, column_mapping)
                
                # Scrape websites
                if urls:
                    scraped_results = self._scrape_websites(urls)
                    
                    # Process scraped content
                    processed_chunk = self._process_chunk_with_scraped_data(
                        chunk, column_mapping, scraped_results
                    )
                else:
                    # No valid URLs in chunk
                    processed_chunk = self._process_chunk_no_urls(chunk, column_mapping)
                
                # Update main DataFrame
                df.iloc[chunk.index] = processed_chunk
                
                processed_rows += len(chunk)
                
                # Update progress
                if progress_callback:
                    progress = (processed_rows / total_rows) * 100
                    progress_callback(progress, f"Processed {processed_rows}/{total_rows} rows")
            
            success_count = len(df[df['processing_status'] == 'success'])
            error_count = len(df[df['processing_status'] == 'error'])
            
            self.logger.info(f"Case B processing completed: {success_count} successful, {error_count} errors")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error in Case B processing: {e}")
            raise
    
    def _identify_columns(self, columns: List[str]) -> Dict[str, str]:
        """Identify required columns in the DataFrame"""
        columns_lower = [col.lower().strip() for col in columns]
        mapping = {
            'website': None,
            'company_name': None
        }
        
        for i, col in enumerate(columns_lower):
            original_col = columns[i]
            
            if ('website' in col or 'url' in col or 'web' in col) and not mapping['website']:
                mapping['website'] = original_col
            elif ('company' in col or 'name' in col or 'business' in col) and not mapping['company_name']:
                mapping['company_name'] = original_col
        
        self.logger.info(f"Column mapping: {mapping}")
        return mapping
    
    def _extract_urls_from_chunk(self, chunk: pd.DataFrame, column_mapping: Dict[str, str]) -> List[str]:
        """Extract and validate URLs from chunk"""
        urls = []
        website_col = column_mapping['website']
        
        for idx, row in chunk.iterrows():
            try:
                website = str(row[website_col]).strip() if pd.notna(row[website_col]) else ""
                
                if website:
                    # Clean and validate URL
                    cleaned_url = self._clean_url(website)
                    if cleaned_url and self._validate_url(cleaned_url):
                        urls.append(cleaned_url)
                        chunk.at[idx, 'scraping_status'] = 'queued'
                    else:
                        chunk.at[idx, 'scraping_status'] = 'invalid_url'
                        chunk.at[idx, 'processing_status'] = 'error'
                else:
                    chunk.at[idx, 'scraping_status'] = 'no_url'
                    chunk.at[idx, 'processing_status'] = 'error'
            
            except Exception as e:
                self.logger.error(f"Error extracting URL from row {idx}: {e}")
                chunk.at[idx, 'scraping_status'] = 'error'
                chunk.at[idx, 'processing_status'] = 'error'
        
        return list(set(urls))  # Remove duplicates
    
    def _clean_url(self, url: str) -> Optional[str]:
        """Clean and format URL"""
        if not url:
            return None
        
        url = url.strip()
        
        # Remove common prefixes people might add
        url = url.replace('www.', '')
        
        # Add https:// if no protocol
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Remove trailing slash
        url = url.rstrip('/')
        
        return url
    
    def _validate_url(self, url: str) -> bool:
        """Validate URL format"""
        try:
            return validators.url(url) is True
        except:
            return False
    
    def _scrape_websites(self, urls: List[str]) -> Dict[str, Dict]:
        """Scrape websites using Scrapy spider"""
        if not urls:
            return {}
        
        try:
            # Create temporary file for URLs
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                json.dump(urls, temp_file)
                temp_file_path = temp_file.name
            
            # Create temporary output file
            output_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            output_file.close()
            
            try:
                # Run Scrapy spider
                spider_command = [
                    'scrapy', 'crawl', 'website_content',
                    '-a', f'urls={",".join(urls)}',
                    '-o', output_file.name,
                    '-s', 'LOG_LEVEL=WARNING'  # Reduce log output
                ]
                
                self.logger.info(f"Starting scraping for {len(urls)} websites")
                
                # Change to scrapy project directory
                original_cwd = os.getcwd()
                scrapy_dir = os.path.join(os.getcwd(), 'scrapy_project')
                os.chdir(scrapy_dir)
                
                try:
                    # Run scraping process
                    result = subprocess.run(
                        spider_command,
                        capture_output=True,
                        text=True,
                        timeout=300  # 5 minutes timeout
                    )
                    
                    if result.returncode != 0:
                        self.logger.error(f"Scrapy process failed: {result.stderr}")
                        return {}
                    
                finally:
                    os.chdir(original_cwd)
                
                # Read scraped results
                scraped_results = {}
                
                if os.path.exists(output_file.name) and os.path.getsize(output_file.name) > 0:
                    with open(output_file.name, 'r', encoding='utf-8') as f:
                        scraped_items = []
                        for line in f:
                            line = line.strip()
                            if line:
                                try:
                                    item = json.loads(line)
                                    scraped_items.append(item)
                                except json.JSONDecodeError:
                                    continue
                    
                    # Organize results by domain
                    for item in scraped_items:
                        domain = item.get('domain', '')
                        if domain:
                            if domain not in scraped_results:
                                scraped_results[domain] = {
                                    'pages': [],
                                    'combined_content': '',
                                    'status': 'success'
                                }
                            
                            scraped_results[domain]['pages'].append(item)
                    
                    # Combine content for each domain
                    for domain, data in scraped_results.items():
                        content_parts = []
                        for page in data['pages']:
                            if page.get('status') == 'scraped' and page.get('content'):
                                content_parts.append(page['content'])
                        
                        data['combined_content'] = ' '.join(content_parts)
                        if not data['combined_content'].strip():
                            data['status'] = 'no_content'
                
                self.logger.info(f"Scraping completed. Results for {len(scraped_results)} domains")
                return scraped_results
                
            finally:
                # Clean up temporary files
                try:
                    os.unlink(temp_file_path)
                    os.unlink(output_file.name)
                except:
                    pass
        
        except Exception as e:
            self.logger.error(f"Error in scraping process: {e}")
            return {}
    
    def _process_chunk_with_scraped_data(self, chunk: pd.DataFrame, column_mapping: Dict[str, str], scraped_results: Dict[str, Dict]) -> pd.DataFrame:
        """Process chunk with scraped website data"""
        website_col = column_mapping['website']
        company_col = column_mapping['company_name']
        
        for idx, row in chunk.iterrows():
            try:
                website = str(row[website_col]).strip() if pd.notna(row[website_col]) else ""
                company_name = str(row[company_col]) if pd.notna(row[company_col]) else ""
                
                if not website:
                    chunk.at[idx, 'processing_status'] = 'error'
                    chunk.at[idx, 'category'] = 'Unknown'
                    chunk.at[idx, 'brand_name'] = 'No website URL provided'
                    continue
                
                # Clean URL and get domain
                cleaned_url = self._clean_url(website)
                if not cleaned_url:
                    chunk.at[idx, 'processing_status'] = 'error'
                    chunk.at[idx, 'category'] = 'Unknown'
                    chunk.at[idx, 'brand_name'] = 'Invalid website URL'
                    continue
                
                domain = urlparse(cleaned_url).netloc
                
                # Check if we have scraped data for this domain
                if domain in scraped_results:
                    scraped_data = scraped_results[domain]
                    content = scraped_data.get('combined_content', '')
                    
                    if content and content.strip():
                        # Extract category and brand name from scraped content using OpenAI
                        result = self.categorizer.categorize_and_extract_brand("", content, company_name)
                        category = result['category']
                        brand_name = result['brand_name']
                        
                        # Update row
                        chunk.at[idx, 'scraped_content'] = content[:1000] + "..." if len(content) > 1000 else content
                        chunk.at[idx, 'category'] = category
                        chunk.at[idx, 'brand_name'] = brand_name
                        chunk.at[idx, 'processing_status'] = 'success'
                        chunk.at[idx, 'scraping_status'] = 'success'
                    else:
                        chunk.at[idx, 'processing_status'] = 'error'
                        chunk.at[idx, 'category'] = 'Unknown'
                        chunk.at[idx, 'brand_name'] = 'No content could be extracted from website'
                        chunk.at[idx, 'scraping_status'] = 'no_content'
                else:
                    chunk.at[idx, 'processing_status'] = 'error'
                    chunk.at[idx, 'category'] = 'Unknown'
                    chunk.at[idx, 'brand_name'] = 'Website could not be scraped'
                    chunk.at[idx, 'scraping_status'] = 'failed'
            
            except Exception as e:
                self.logger.error(f"Error processing row {idx} with scraped data: {e}")
                chunk.at[idx, 'processing_status'] = 'error'
                chunk.at[idx, 'category'] = 'Unknown'
                chunk.at[idx, 'brand_name'] = 'Error processing website data'
                chunk.at[idx, 'scraping_status'] = 'error'
        
        return chunk
    
    def _process_chunk_no_urls(self, chunk: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """Process chunk when no valid URLs are found"""
        for idx, row in chunk.iterrows():
            chunk.at[idx, 'scraped_content'] = ''
            chunk.at[idx, 'category'] = 'Unknown'
            chunk.at[idx, 'brand_name'] = 'No valid website URL provided'
            chunk.at[idx, 'processing_status'] = 'error'
            chunk.at[idx, 'scraping_status'] = 'no_valid_urls'
        
        return chunk