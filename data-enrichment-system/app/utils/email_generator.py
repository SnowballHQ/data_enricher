import logging
from typing import Dict, Optional

class EmailGenerator:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_template = "When we asked ChatGPT about {category} brands [brands like yours], it listed several competitors, but {{companyName}} didn't appear."
        
        # Category-specific variations
        self.category_variations = {
            'SaaS': '{category} solutions',
            'E-commerce': '{category} platforms',
            'Healthcare': '{category} companies',
            'Manufacturing': '{category} businesses', 
            'Consulting': '{category} firms',
            'Financial Services': '{category} companies',
            'Education': '{category} platforms',
            'Real Estate': '{category} companies',
            'Technology': '{category} companies',
            'Marketing': '{category} agencies'
        }
        
        # Alternative templates for variety
        self.alternative_templates = [
            "When we asked ChatGPT about {category} brands [brands like yours], it listed several competitors, but {{companyName}} didn't appear.",
            "We recently asked ChatGPT to list top {category} brands [companies like yours], and while it mentioned several competitors, {{companyName}} wasn't included.",
            "ChatGPT listed multiple {category} brands [similar to yours] when we asked, but {{companyName}} wasn't among them.",
            "We queried ChatGPT about leading {category} brands [brands in your space], and while it listed several competitors, {{companyName}} was missing."
        ]
    
    def generate_email_snippet(self, category: str, company_name: str = "", template_index: int = 0) -> str:
        """Generate personalized cold email snippet"""
        try:
            # Handle unknown or empty categories
            if not category or category.lower() in ['unknown', 'none', '']:
                category = "industry"
            
            # Get category variation if available
            category_text = self.category_variations.get(category, category)
            
            # Format category text
            category_formatted = category_text.format(category=category.lower())
            
            # Select template
            template_index = min(template_index, len(self.alternative_templates) - 1)
            template = self.alternative_templates[template_index]
            
            # Generate snippet
            snippet = template.format(category=category_formatted)
            
            self.logger.info(f"Generated email snippet for category: {category}")
            return snippet
            
        except Exception as e:
            self.logger.error(f"Error generating email snippet: {e}")
            # Return fallback snippet
            return "When we asked ChatGPT about industry brands [brands like yours], it listed several competitors, but {{companyName}} didn't appear."
    
    def generate_multiple_variations(self, category: str, count: int = 3) -> list:
        """Generate multiple email snippet variations"""
        variations = []
        
        for i in range(min(count, len(self.alternative_templates))):
            snippet = self.generate_email_snippet(category, template_index=i)
            variations.append(snippet)
        
        return variations
    
    def customize_for_industry(self, category: str, industry_context: str = "") -> str:
        """Generate industry-specific customized snippet"""
        try:
            base_snippet = self.generate_email_snippet(category)
            
            # Add industry context if provided
            if industry_context:
                context_additions = {
                    'b2b': 'in the B2B space',
                    'enterprise': 'in the enterprise market',
                    'startup': 'in the startup ecosystem',
                    'fortune500': 'among Fortune 500 companies',
                    'smb': 'in the SMB market'
                }
                
                for context_key, addition in context_additions.items():
                    if context_key.lower() in industry_context.lower():
                        # Insert context into snippet
                        base_snippet = base_snippet.replace(
                            'brands [brands like yours]',
                            f'brands {addition} [brands like yours]'
                        )
                        break
            
            return base_snippet
            
        except Exception as e:
            self.logger.error(f"Error customizing snippet: {e}")
            return self.generate_email_snippet(category)
    
    def validate_snippet(self, snippet: str) -> Dict[str, any]:
        """Validate generated email snippet"""
        validation = {
            'valid': True,
            'issues': [],
            'word_count': 0,
            'has_placeholder': False
        }
        
        try:
            # Check if snippet exists and is not empty
            if not snippet or not snippet.strip():
                validation['valid'] = False
                validation['issues'].append('Snippet is empty')
                return validation
            
            # Count words
            validation['word_count'] = len(snippet.split())
            
            # Check for company name placeholder
            validation['has_placeholder'] = '{{companyName}}' in snippet
            
            # Check word count (should be reasonable length)
            if validation['word_count'] < 10:
                validation['issues'].append('Snippet too short')
            elif validation['word_count'] > 50:
                validation['issues'].append('Snippet too long')
            
            # Check for placeholder
            if not validation['has_placeholder']:
                validation['issues'].append('Missing company name placeholder')
            
            # Check for basic structure
            required_elements = ['ChatGPT', 'brands', 'competitors']
            for element in required_elements:
                if element.lower() not in snippet.lower():
                    validation['issues'].append(f'Missing required element: {element}')
            
            if validation['issues']:
                validation['valid'] = False
            
        except Exception as e:
            validation['valid'] = False
            validation['issues'].append(f'Validation error: {str(e)}')
            self.logger.error(f"Snippet validation error: {e}")
        
        return validation