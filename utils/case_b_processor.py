"""
Case B Processor for handling website URLs and scraping content
"""

import pandas as pd
import logging
import subprocess
import json
import os
import tempfile
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import validators
from utils.openai_categorizer import OpenAICategorizer


class CaseBProcessor:
    """Processor for Case B: Files with website URLs requiring scraping"""
    
    def __init__(self, openai_api_key: str):
        self.logger = logging.getLogger(__name__)
        self.categorizer = OpenAICategorizer(openai_api_key)
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
            df['email_question'] = ''
            df['processing_status'] = 'pending'
            df['scraping_status'] = 'pending'
            
            total_rows = len(df)
            processed_rows = 0
            
            self.logger.info(f"Starting Case B processing for {total_rows} rows")
            
            # Process in chunks
            chunk_size = min(100, total_rows)  # Process 100 rows at a time
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
        """Scrape websites using fallback method (Scrapy not working on Windows)"""
        if not urls:
            return {}
        
        try:
            # Always use fallback method for Windows compatibility
            self.logger.info("Using fallback scraping method for Windows compatibility")
            return self._fallback_scraping(urls)
        
        except Exception as e:
            self.logger.error(f"Error in scraping process: {e}")
            return {}
    
    def _fallback_scraping(self, urls: List[str]) -> Dict[str, Dict]:
        """Fallback scraping method using basic HTML parsing"""
        import requests
        import re
        import time
        
        self.logger.info("Using simple fallback scraping method with requests")
        scraped_results = {}
        
        # Request headers to appear more like a real browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        for url in urls:
            try:
                domain = urlparse(url).netloc
                self.logger.info(f"Scraping {url} using requests")
                
                # Make request with timeout
                response = requests.get(url, headers=headers, timeout=30, verify=False)
                response.raise_for_status()
                
                html_content = response.text
                
                # Extract title using regex
                title = ""
                title_match = re.search(r'<title[^>]*>(.*?)</title>', html_content, re.IGNORECASE | re.DOTALL)
                if title_match:
                    title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
                
                # Extract meta description using regex
                meta_desc = ""
                meta_match = re.search(r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']*)["\']', html_content, re.IGNORECASE)
                if meta_match:
                    meta_desc = meta_match.group(1).strip()
                
                # Remove script and style tags
                html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.IGNORECASE | re.DOTALL)
                html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.IGNORECASE | re.DOTALL)
                html_content = re.sub(r'<nav[^>]*>.*?</nav>', '', html_content, flags=re.IGNORECASE | re.DOTALL)
                html_content = re.sub(r'<header[^>]*>.*?</header>', '', html_content, flags=re.IGNORECASE | re.DOTALL)
                html_content = re.sub(r'<footer[^>]*>.*?</footer>', '', html_content, flags=re.IGNORECASE | re.DOTALL)
                
                # Extract text content by removing HTML tags
                text_content = re.sub(r'<[^>]+>', ' ', html_content)
                
                # Clean up text content
                text_content = re.sub(r'\s+', ' ', text_content)  # Normalize whitespace
                text_content = text_content.strip()
                
                # Filter out common navigation and boilerplate text
                lines = text_content.split('.')
                filtered_lines = []
                nav_terms = ['home', 'about', 'contact', 'menu', 'login', 'signup', 'search', 'privacy', 'terms', 'cookies']
                
                for line in lines:
                    line = line.strip()
                    if len(line) > 20:  # Only include substantial content
                        # Skip lines that are mostly navigation
                        if not any(term in line.lower() for term in nav_terms):
                            filtered_lines.append(line)
                
                # Combine filtered content
                combined_content = '. '.join(filtered_lines)
                
                # Add title and meta description if available
                content_parts = []
                if title:
                    content_parts.append(f"Title: {title}")
                if meta_desc:
                    content_parts.append(f"Description: {meta_desc}")
                if combined_content:
                    content_parts.append(combined_content)
                
                final_content = '. '.join(content_parts)
                
                # Limit content length
                max_length = 5000
                if len(final_content) > max_length:
                    final_content = final_content[:max_length] + "..."
                
                # Only consider it successful if we got some meaningful content
                if len(final_content.strip()) > 100:
                    status = 'success'
                else:
                    status = 'no_content'
                    final_content = f"Limited content extracted from {domain}"
                
                scraped_results[domain] = {
                    'pages': [{
                        'url': url,
                        'title': title,
                        'meta_description': meta_desc,
                        'content': final_content,
                        'status': 'scraped'
                    }],
                    'combined_content': final_content,
                    'status': status
                }
                
                self.logger.info(f"Successfully scraped {url} - extracted {len(final_content)} characters")
                
                # Small delay to be respectful
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request error for {url}: {e}")
                domain = urlparse(url).netloc if url else "unknown"
                scraped_results[domain] = {
                    'pages': [],
                    'combined_content': "",
                    'status': 'error',
                    'error': str(e)
                }
            except Exception as e:
                self.logger.error(f"Error in fallback scraping for {url}: {e}")
                domain = urlparse(url).netloc if url else "unknown"
                scraped_results[domain] = {
                    'pages': [],
                    'combined_content': "",
                    'status': 'error',
                    'error': str(e)
                }
        
        return scraped_results
    
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
                        # Extract category, brand name, and email question from scraped content using OpenAI
                        result = self.categorizer.categorize_and_extract_brand("", content, company_name)
                        category = result['category']
                        brand_name = result['brand_name']
                        email_question = result.get('email_question', 'What are the best local service providers?')
                        
                        # Update row
                        chunk.at[idx, 'scraped_content'] = content[:1000] + "..." if len(content) > 1000 else content
                        chunk.at[idx, 'category'] = category
                        chunk.at[idx, 'brand_name'] = brand_name
                        chunk.at[idx, 'email_question'] = email_question
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
