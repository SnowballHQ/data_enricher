import scrapy
import logging
from urllib.parse import urljoin, urlparse
from datetime import datetime

from scrapy_project.items import WebsiteContentItem
from config.settings import PAGES_TO_SCRAPE, CONTENT_SELECTORS, IGNORE_SELECTORS

class WebsiteSpider(scrapy.Spider):
    name = 'website_content'
    allowed_domains = []
    start_urls = []
    
    def __init__(self, urls=None, *args, **kwargs):
        super(WebsiteSpider, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        
        if urls:
            # Handle both single URL and list of URLs
            if isinstance(urls, str):
                urls = [urls]
            
            self.start_urls = []
            self.allowed_domains = []
            
            for url in urls:
                # Ensure URL has protocol
                if not url.startswith(('http://', 'https://')):
                    url = 'https://' + url
                
                # Add to start URLs
                self.start_urls.append(url)
                
                # Add domain to allowed domains
                parsed_url = urlparse(url)
                domain = parsed_url.netloc.lower()
                if domain and domain not in self.allowed_domains:
                    self.allowed_domains.append(domain)
        
        self.logger.info(f"Spider initialized with {len(self.start_urls)} URLs")
    
    def start_requests(self):
        """Generate initial requests for all URLs and their sub-pages"""
        for url in self.start_urls:
            try:
                # Request homepage
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={'page_type': 'homepage'},
                    errback=self.handle_error
                )
                
                # Request additional pages
                for page_path in PAGES_TO_SCRAPE[1:]:  # Skip '/' as it's the homepage
                    page_url = urljoin(url, page_path)
                    yield scrapy.Request(
                        url=page_url,
                        callback=self.parse,
                        meta={'page_type': page_path.strip('/')},
                        errback=self.handle_error,
                        dont_filter=True  # Allow multiple requests to same domain
                    )
            
            except Exception as e:
                self.logger.error(f"Error creating request for {url}: {e}")
    
    def parse(self, response):
        """Parse website content"""
        try:
            item = WebsiteContentItem()
            item['url'] = response.url
            item['domain'] = urlparse(response.url).netloc
            item['page_type'] = response.meta.get('page_type', 'unknown')
            item['scraped_at'] = datetime.now().isoformat()
            
            # Extract title
            item['title'] = self._extract_title(response)
            
            # Extract meta description
            item['meta_description'] = self._extract_meta_description(response)
            
            # Extract main content
            item['content'] = self._extract_content(response)
            
            # Set status
            item['status'] = 'scraped'
            
            self.logger.info(f"Successfully scraped {response.url} ({item['page_type']})")
            
            yield item
        
        except Exception as e:
            self.logger.error(f"Error parsing {response.url}: {e}")
            yield self._create_error_item(response, str(e))
    
    def handle_error(self, failure):
        """Handle request errors"""
        request = failure.request
        self.logger.error(f"Request failed for {request.url}: {failure.value}")
        
        # Create error item
        item = WebsiteContentItem()
        item['url'] = request.url
        item['domain'] = urlparse(request.url).netloc
        item['page_type'] = request.meta.get('page_type', 'unknown')
        item['scraped_at'] = datetime.now().isoformat()
        item['status'] = 'error'
        item['error_message'] = str(failure.value)
        
        yield item
    
    def _extract_title(self, response) -> str:
        """Extract page title"""
        try:
            title = response.css('title::text').get()
            if title:
                return title.strip()
            
            # Fallback to h1
            h1 = response.css('h1::text').get()
            if h1:
                return h1.strip()
            
            return ""
        except:
            return ""
    
    def _extract_meta_description(self, response) -> str:
        """Extract meta description"""
        try:
            meta_desc = response.css('meta[name="description"]::attr(content)').get()
            if meta_desc:
                return meta_desc.strip()
            return ""
        except:
            return ""
    
    def _extract_content(self, response) -> str:
        """Extract main content from page"""
        try:
            content_parts = []
            
            # Try to find main content areas
            for selector in CONTENT_SELECTORS:
                content_elements = response.css(selector)
                if content_elements:
                    for element in content_elements:
                        # Remove ignored elements
                        for ignore_selector in IGNORE_SELECTORS:
                            for ignored in element.css(ignore_selector):
                                ignored.remove()
                        
                        # Extract text
                        text = element.css('::text').getall()
                        if text:
                            clean_text = ' '.join([t.strip() for t in text if t.strip()])
                            if clean_text:
                                content_parts.append(clean_text)
            
            # If no main content found, try body
            if not content_parts:
                body_text = response.css('body ::text').getall()
                if body_text:
                    # Filter out navigation and other unwanted text
                    filtered_text = []
                    for text in body_text:
                        text = text.strip()
                        if text and len(text) > 3:  # Ignore very short text
                            # Skip common navigation terms
                            nav_terms = ['home', 'about', 'contact', 'menu', 'login', 'signup', 'search']
                            if text.lower() not in nav_terms:
                                filtered_text.append(text)
                    
                    content_parts.append(' '.join(filtered_text))
            
            # Combine all content
            full_content = ' '.join(content_parts)
            
            # Clean up content
            full_content = ' '.join(full_content.split())  # Normalize whitespace
            
            # Limit content length (optional)
            max_length = 10000  # Adjust as needed
            if len(full_content) > max_length:
                full_content = full_content[:max_length] + "..."
            
            return full_content
        
        except Exception as e:
            self.logger.error(f"Error extracting content: {e}")
            return ""
    
    def _create_error_item(self, response, error_message: str) -> WebsiteContentItem:
        """Create error item"""
        item = WebsiteContentItem()
        item['url'] = response.url
        item['domain'] = urlparse(response.url).netloc
        item['page_type'] = response.meta.get('page_type', 'unknown')
        item['scraped_at'] = datetime.now().isoformat()
        item['status'] = 'error'
        item['error_message'] = error_message
        item['title'] = ""
        item['content'] = ""
        item['meta_description'] = ""
        
        return item