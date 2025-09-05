"""
OpenAI API-based product categorization utility
"""

import openai
import os
import time
import threading
import json
from typing import Optional, List, Dict, Tuple
from config import OPENAI_MODEL, OPENAI_MAX_TOKENS, OPENAI_TEMPERATURE

class OpenAICategorizer:
    def __init__(self, api_key: str):
        """Initialize the OpenAI categorizer with API key"""
        # Set the API key for the openai module
        openai.api_key = api_key
        self._request_lock = threading.Lock()  # Thread safety for rate limiting
    
    def categorize_and_extract_brand(self, keywords: str, description: str, company_context: str = "") -> Dict[str, str]:
        """
        Categorize a product and extract cleaned brand name using OpenAI API
        
        Args:
            keywords: Product keywords
            description: Product description
            company_context: Additional company context (optional)
            
        Returns:
            Dict: {'category': str, 'brand_name': str}
            
        Raises:
            Exception: If OpenAI API fails
        """
        try:
            print(f"🔍 DEBUG - Making OpenAI API call...")
            print(f"🔍 DEBUG - Using model: {OPENAI_MODEL}")
            print(f"🔍 DEBUG - Max tokens: {OPENAI_MAX_TOKENS}")
            print(f"🔍 DEBUG - Temperature: {OPENAI_TEMPERATURE}")
            
            # Create the prompt for categorization and brand extraction
            prompt = self._create_categorization_and_brand_prompt(keywords, description, company_context)
            print(f"🔍 DEBUG - Prompt created, length: {len(prompt)} characters")
            
            # Make API call to OpenAI using the correct method for v0.28.1
            print(f"🔍 DEBUG - Sending request to OpenAI...")
            
            # Add thread safety and rate limiting
            with self._request_lock:
                # Small delay to avoid hitting rate limits
                time.sleep(0.2)  # Increased to 200ms delay between requests
                
                # Add timeout and retry logic
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        print(f"🔍 DEBUG - API attempt {attempt + 1}/{max_retries}")
                        response = openai.ChatCompletion.create(
                            model=OPENAI_MODEL,
                            messages=[
                                {
                                    "role": "system",
                                    "content": "You are a product categorization and brand extraction expert. Your task is to analyze the product information and return both the most appropriate business category AND the cleaned official company/brand name. You must return a valid JSON object with exactly two fields: 'category' and 'brand_name'. No additional text or explanation."
                                },
                                {
                                    "role": "user",
                                    "content": prompt
                                }
                            ],
                            request_timeout=30  # 30 second timeout
                        )
                        break  # Success, exit retry loop
                    except Exception as api_error:
                        print(f"⚠️ DEBUG - API attempt {attempt + 1} failed: {str(api_error)}")
                        if attempt == max_retries - 1:
                            raise api_error  # Re-raise on final attempt
                        else:
                            # Wait longer before retry
                            wait_time = (attempt + 1) * 2  # 2, 4, 6 seconds
                            print(f"🔍 DEBUG - Waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
            
            print(f"✅ DEBUG - Received response from OpenAI")
            
            # Extract and parse the JSON response
            raw_response = response.choices[0].message.content.strip()
            print(f"🔍 DEBUG - OpenAI returned raw response: '{raw_response}'")
            
            try:
                # Parse JSON response
                result = json.loads(raw_response)
                
                # Validate required fields
                if 'category' not in result or 'brand_name' not in result:
                    print(f"❌ Missing required fields in response: {result}")
                    # Fallback: extract what we can
                    category = result.get('category', 'Unknown Category')
                    brand_name = result.get('brand_name', 'Unknown Brand')
                else:
                    category = result['category'].strip()
                    brand_name = result['brand_name'].strip()
                
                print(f"🔍 DEBUG - Parsed category: '{category}'")
                print(f"🔍 DEBUG - Parsed brand_name: '{brand_name}'")
                
                return {
                    'category': category,
                    'brand_name': brand_name
                }
                
            except json.JSONDecodeError as json_error:
                print(f"❌ Failed to parse JSON response: {json_error}")
                print(f"❌ Raw response was: {raw_response}")
                
                # Fallback: try to extract category and brand from non-JSON response
                lines = raw_response.split('\n')
                category = "Unknown Category"
                brand_name = "Unknown Brand"
                
                for line in lines:
                    if 'category' in line.lower():
                        category = line.split(':')[-1].strip().strip('"').strip("'")
                    elif 'brand' in line.lower() or 'company' in line.lower():
                        brand_name = line.split(':')[-1].strip().strip('"').strip("'")
                
                return {
                    'category': category,
                    'brand_name': brand_name
                }
                
        except Exception as e:
            print(f"❌ Error in OpenAI categorization: {e}")
            print(f"❌ Error type: {type(e).__name__}")
            # Re-raise the exception
            raise
    
    def _create_categorization_and_brand_prompt(self, keywords: str, description: str, company_context: str = "") -> str:
        """Create the prompt for OpenAI API to extract both category and brand name"""
        return f"""
        Please analyze this product/company information and extract two things:
        1. The most appropriate business category
        2. The official company/brand name (cleaned and standardized)
        
        Product Keywords: {keywords}
        Product Description: {description}
        Company Context: {company_context}
        
        For the category, think about:
        - What industry or sector does this belong to?
        - What type of business is this?
        - What would be the most specific and accurate category?
        
        For the brand name, extract and clean:
        - Remove URLs, promotional text, extra words
        - Use the official name the company refers to itself
        - Standardize capitalization and formatting
        - Remove "Inc.", "LLC", "Ltd." unless part of the official brand
        
        Return ONLY a valid JSON object with this exact format:
        {{
            "category": "Business Category Here",
            "brand_name": "Cleaned Company Name Here"
        }}
        
        Example categories: "SaaS Software", "E-commerce Platform", "Healthcare Technology", "Financial Services", "Manufacturing Equipment", "Consulting Services"
        Example brand names: "Microsoft", "Tesla", "Shopify", "Airbnb"
        """
    
    def categorize_product(self, keywords: str, description: str) -> str:
        """
        Backward compatibility method - returns only category
        
        Args:
            keywords: Product keywords
            description: Product description
            
        Returns:
            str: AI-generated product category
        """
        result = self.categorize_and_extract_brand(keywords, description)
        return result['category']
    
    def batch_categorize(self, products: List[dict]) -> List[str]:
        """
        Categorize multiple products in batch (backward compatibility - returns only categories)
        
        Args:
            products: List of dictionaries with 'keywords' and 'description' keys
            
        Returns:
            List of category strings
            
        Raises:
            Exception: If any product categorization fails
        """
        categories = []
        for product in products:
            keywords = product.get('keywords', '')
            description = product.get('description', '')
            category = self.categorize_product(keywords, description)
            categories.append(category)
        return categories
    
    def batch_categorize_and_extract_brands(self, products: List[dict]) -> List[Dict[str, str]]:
        """
        Categorize multiple products and extract brand names in batch
        
        Args:
            products: List of dictionaries with 'keywords', 'description', and optional 'company_context' keys
            
        Returns:
            List of dictionaries with 'category' and 'brand_name' keys
            
        Raises:
            Exception: If any product processing fails
        """
        results = []
        for product in products:
            keywords = product.get('keywords', '')
            description = product.get('description', '')
            company_context = product.get('company_context', '')
            result = self.categorize_and_extract_brand(keywords, description, company_context)
            results.append(result)
        return results
