BOT_NAME = 'website_scraper'

SPIDER_MODULES = ['scrapy_project.spiders']
NEWSPIDER_MODULE = 'scrapy_project.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure delays for requests
RANDOMIZE_DOWNLOAD_DELAY = 0.5
DOWNLOAD_DELAY = 1

# Configure concurrent requests
CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 2

# Configure user agent
USER_AGENT = 'data-enrichment-bot (+http://www.yourdomain.com)'

# Configure pipelines
ITEM_PIPELINES = {
    'scrapy_project.pipelines.ContentCleaningPipeline': 300,
}

# Configure request fingerprinting
REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'

# AutoThrottle settings
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 60
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = False

# Retry settings
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

# Cookies and sessions
COOKIES_ENABLED = False

# Disable Telnet Console
TELNETCONSOLE_ENABLED = False

# Configure logging
LOG_LEVEL = 'INFO'

# Timeout settings
DOWNLOAD_TIMEOUT = 30

# Configure headers
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en',
    'Accept-Encoding': 'gzip, deflate',
}