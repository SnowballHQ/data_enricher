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
        self.api_key = api_key  # Store as instance attribute for access by other classes
        self._request_lock = threading.Lock()  # Thread safety for rate limiting
    
    def categorize_and_extract_brand(self, keywords: str, description: str, company_context: str = "") -> Dict[str, str]:
        """
        Categorize a product, extract cleaned brand name, and generate email question using OpenAI API
        
        Args:
            keywords: Product keywords
            description: Product description
            company_context: Additional company context (optional)
            
        Returns:
            Dict: {'category': str, 'brand_name': str, 'email_question': str}
            
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
                                    "content": "You are a product categorization and brand extraction expert. Your task is to analyze the product information and return the business category, cleaned company name, AND a personalized email question. You must return a valid JSON object with exactly three fields: 'category', 'brand_name', and 'email_question'. No additional text or explanation."
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
                if 'category' not in result or 'brand_name' not in result or 'email_question' not in result:
                    print(f"❌ Missing required fields in response: {result}")
                    # Fallback: extract what we can
                    category = result.get('category', 'Unknown Category')
                    brand_name = result.get('brand_name', 'Unknown Brand')
                    email_question = result.get('email_question', 'What are the best local brands?')
                else:
                    category = result['category'].strip()
                    brand_name = result['brand_name'].strip()
                    email_question = result['email_question'].strip()
                
                print(f"🔍 DEBUG - Parsed category: '{category}'")
                print(f"🔍 DEBUG - Parsed brand_name: '{brand_name}'")
                print(f"🔍 DEBUG - Parsed email_question: '{email_question}'")
                
                return {
                    'category': category,
                    'brand_name': brand_name,
                    'email_question': email_question
                }
                
            except json.JSONDecodeError as json_error:
                print(f"❌ Failed to parse JSON response: {json_error}")
                print(f"❌ Raw response was: {raw_response}")
                
                # Fallback: try to extract category and brand from non-JSON response
                lines = raw_response.split('\n')
                category = "Unknown Category"
                brand_name = "Unknown Brand"
                email_question = "What are the best local brands?"
                
                for line in lines:
                    if 'category' in line.lower():
                        category = line.split(':')[-1].strip().strip('"').strip("'")
                    elif 'brand' in line.lower() or 'company' in line.lower():
                        brand_name = line.split(':')[-1].strip().strip('"').strip("'")
                    elif 'question' in line.lower() or 'email' in line.lower():
                        email_question = line.split(':')[-1].strip().strip('"').strip("'")
                
                return {
                    'category': category,
                    'brand_name': brand_name,
                    'email_question': email_question
                }
                
        except Exception as e:
            print(f"❌ Error in OpenAI categorization: {e}")
            print(f"❌ Error type: {type(e).__name__}")
            # Re-raise the exception
            raise
    
    def _create_categorization_and_brand_prompt(self, keywords: str, description: str, company_context: str = "") -> str:
        """Create the prompt for OpenAI API to extract category, brand name, and email question"""
        return f"""
        Please analyze this product/company information and extract three things:
        1. A HIGHLY SPECIFIC business category (2-4 words max)
        2. The official company/brand name (cleaned and standardized)
        3. A personalized email question for cold outreach
        
        Product Keywords: {keywords}
        Product Description: {description}
        Company Context: {company_context}
        
        For the category, be VERY SPECIFIC (2-4 words):
        - What exact product or service do they sell?
        - Include qualifiers like "Independent", "Family-owned", "Custom", "Local" when relevant
        - AVOID generic terms like "retail", "e-commerce", "services", "solutions", "company"
        - Focus on the actual product/service offered
        
        For the brand name, extract and clean:
        - Remove URLs, promotional text, extra words
        - Use the official name the company refers to itself
        - Standardize capitalization and formatting
        - Remove "Inc.", "LLC", "Ltd." unless part of the official brand
        
        For the email question, create a question their potential customers would ask ChatGPT:
        - Think about what someone would search for when they need this product/service
        - Focus on the customer's problem or need, not the company name
        - Make it a question someone would ask to discover companies like theirs
        - If it's a local business, include location (city, state, region) in the question
        - Examples: "What are healthy pasta alternatives for weight loss?", "Where can I find organic dental care in San Francisco?", "Best places to buy eco-friendly camping gear in Colorado?"
        - Avoid mentioning the specific company name
        - Make it discovery-focused from a customer perspective
        
        EXAMPLES:
        
        Input: Hardware store association in California
        Output: {{
            "category": "Independent Hardware Stores",
            "brand_name": "CRHWA",
            "email_question": "Where can I find independent hardware stores in California that aren't big box retailers?"
        }}
        
        Input: Family shoe store in San Francisco Bay Area
        Output: {{
            "category": "Family Shoe Stores", 
            "brand_name": "Hansen's Shoes",
            "email_question": "Best family-owned shoe stores in San Francisco Bay Area with personalized service?"
        }}
        
        Input: RV gear and camping accessories online
        Output: {{
            "category": "RV Camping Gear",
            "brand_name": "Hitched4Fun",
            "email_question": "Where to buy specialized RV camping equipment and accessories online?"
        }}
        
        Input: Zero-waste refill store in Portland neighborhood
        Output: {{
            "category": "Zero-Waste Refill Stores",
            "brand_name": "Simple",
            "email_question": "Where can I buy household products without packaging in Portland to reduce waste?"
        }}
        
        Return ONLY a valid JSON object with this exact format:
        {{
            "category": "Specific 2-4 Word Category",
            "brand_name": "Cleaned Company Name",
            "email_question": "What are the best [location/qualifier] [category] brands?"
        }}
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
            List of dictionaries with 'category', 'brand_name', and 'email_question' keys
            
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
