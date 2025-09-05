import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
OPENAI_MODEL = 'gpt-5-nano'

# File Processing
MAX_FILE_SIZE_MB = 50
CHUNK_SIZE = 100
SUPPORTED_FORMATS = ['csv', 'xlsx', 'xls']

# Web Scraping
SCRAPY_SETTINGS = {
    'USER_AGENT': 'data-enrichment-bot 1.0',
    'ROBOTSTXT_OBEY': True,
    'CONCURRENT_REQUESTS': 16,
    'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
    'DOWNLOAD_DELAY': 1,
    'RANDOMIZE_DOWNLOAD_DELAY': 0.5,
    'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
}

# Content Filtering
IGNORE_SELECTORS = [
    'nav', 'header', 'footer', 'aside', 
    '.cookie-banner', '.popup', '.advertisement',
    'script', 'style', 'iframe', 'noscript',
    '.social-media', '.newsletter', '.sidebar'
]

CONTENT_SELECTORS = [
    'main', '[role="main"]', '.content', 
    'article', 'section', '.main-content',
    '.page-content', '#content'
]

# Pages to scrape
PAGES_TO_SCRAPE = ['/', '/about', '/about-us', '/products', '/services', '/solutions']

# Output Configuration
OUTPUT_COLUMNS = {
    'CASE_A': ['company_name', 'keywords', 'description', 'category', 'brand_name'],
    'CASE_B': ['company_name', 'website', 'scraped_content', 'category', 'brand_name']
}

# Logging
LOG_LEVEL = 'INFO'
LOG_FILE = 'logs/app.log'