"""
Configuration file for the Product Categorization System
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Email template for personalization - now handles dynamic categories
#EMAIL_TEMPLATE = "When we asked ChatGPT about {product_category} brands, it listed several competitors, but {company_name} didn't appear."

# File processing settings
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
SUPPORTED_FORMATS = ['.xlsx', '.xls', '.csv']
CHUNK_SIZE = 1000  # Process data in chunks for large files

# OpenAI API settings
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-5-nano')
OPENAI_MAX_TOKENS = int(os.getenv('OPENAI_MAX_TOKENS', '100'))
OPENAI_TEMPERATURE = float(os.getenv('OPENAI_TEMPERATURE', '0.1'))

def get_openai_api_key():
    """Get OpenAI API key from environment variables"""
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OpenAI API key not found. Please set OPENAI_API_KEY in your .env file or environment variables.")
    return api_key

def is_openai_configured():
    """Check if OpenAI is properly configured"""
    try:
        get_openai_api_key()
        return True
    except ValueError:
        return False
