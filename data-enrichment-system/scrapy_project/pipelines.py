import re
import logging
from bs4 import BeautifulSoup
from scrapy_project.items import WebsiteContentItem

class ContentCleaningPipeline:
    """Pipeline to clean and process scraped content"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_item(self, item: WebsiteContentItem, spider):
        """Clean and process the scraped content"""
        try:
            if item.get('content'):
                # Clean HTML content
                cleaned_content = self._clean_html_content(item['content'])
                
                # Remove unwanted sections
                cleaned_content = self._remove_unwanted_sections(cleaned_content)
                
                # Normalize whitespace
                cleaned_content = self._normalize_whitespace(cleaned_content)
                
                # Update item
                item['content'] = cleaned_content
                
                # Set status
                item['status'] = 'success' if cleaned_content.strip() else 'empty_content'
            else:
                item['status'] = 'no_content'
                item['error_message'] = 'No content extracted'
        
        except Exception as e:
            item['status'] = 'processing_error'
            item['error_message'] = str(e)
            self.logger.error(f"Error processing item: {e}")
        
        return item
    
    def _clean_html_content(self, html_content: str) -> str:
        """Clean HTML content and extract meaningful text"""
        if not html_content:
            return ""
        
        try:
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove unwanted tags
            unwanted_tags = [
                'script', 'style', 'nav', 'header', 'footer', 
                'aside', 'iframe', 'noscript', 'object', 'embed'
            ]
            
            for tag in unwanted_tags:
                for element in soup.find_all(tag):
                    element.decompose()
            
            # Remove unwanted classes and IDs
            unwanted_selectors = [
                '.cookie-banner', '.popup', '.modal', '.advertisement',
                '.social-media', '.newsletter', '.sidebar', '.menu',
                '#cookie-notice', '#popup', '#advertisement'
            ]
            
            for selector in unwanted_selectors:
                if selector.startswith('.'):
                    class_name = selector[1:]
                    for element in soup.find_all(class_=lambda x: x and class_name in x):
                        element.decompose()
                elif selector.startswith('#'):
                    id_name = selector[1:]
                    element = soup.find(id=id_name)
                    if element:
                        element.decompose()
            
            # Extract text
            text = soup.get_text(separator=' ', strip=True)
            return text
            
        except Exception as e:
            self.logger.error(f"Error cleaning HTML: {e}")
            return html_content
    
    def _remove_unwanted_sections(self, text: str) -> str:
        """Remove unwanted sections from text"""
        if not text:
            return ""
        
        # Common unwanted patterns
        unwanted_patterns = [
            r'cookie\s+policy.*?(?=\.|$)',
            r'privacy\s+policy.*?(?=\.|$)',
            r'terms\s+of\s+service.*?(?=\.|$)',
            r'subscribe\s+to.*?newsletter.*?(?=\.|$)',
            r'follow\s+us\s+on.*?(?=\.|$)',
            r'copyright.*?(?=\.|$)',
            r'all\s+rights\s+reserved.*?(?=\.|$)',
        ]
        
        for pattern in unwanted_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        return text
    
    def _normalize_whitespace(self, text: str) -> str:
        """Normalize whitespace in text"""
        if not text:
            return ""
        
        # Replace multiple whitespace with single space
        text = re.sub(r'\s+', ' ', text)
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        return text