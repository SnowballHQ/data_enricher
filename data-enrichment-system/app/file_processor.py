import pandas as pd
import os
import logging
from typing import Dict, List, Optional, Tuple
from config.settings import SUPPORTED_FORMATS, MAX_FILE_SIZE_MB

class FileProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def detect_file_type(self, file_path: str) -> str:
        """Detect if file contains keywords/description or only website URLs"""
        try:
            # Read file based on extension
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
            
            # Get column names (case insensitive)
            columns = [col.lower().strip() for col in df.columns]
            
            # Check for Case A indicators
            has_keywords = any('keyword' in col for col in columns)
            has_description = any('description' in col or 'desc' in col for col in columns)
            has_website = any('website' in col or 'url' in col or 'web' in col for col in columns)
            has_company = any('company' in col or 'name' in col or 'business' in col for col in columns)
            
            self.logger.info(f"Column analysis - Keywords: {has_keywords}, Description: {has_description}, Website: {has_website}, Company: {has_company}")
            
            if has_keywords and has_description:
                return "CASE_A"
            elif has_website:
                return "CASE_B"
            else:
                return "UNKNOWN"
                
        except Exception as e:
            self.logger.error(f"Error detecting file type: {e}")
            raise
    
    def validate_file(self, file_path: str) -> Dict[str, any]:
        """Validate uploaded file"""
        validation_result = {
            'valid': False,
            'file_size': 0,
            'rows': 0,
            'columns': [],
            'case_type': None,
            'errors': []
        }
        
        try:
            # Check file existence
            if not os.path.exists(file_path):
                validation_result['errors'].append("File does not exist")
                return validation_result
            
            # Check file size
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            validation_result['file_size'] = file_size_mb
            
            if file_size_mb > MAX_FILE_SIZE_MB:
                validation_result['errors'].append(f"File too large: {file_size_mb:.2f}MB (max: {MAX_FILE_SIZE_MB}MB)")
                return validation_result
            
            # Check file format
            file_ext = file_path.split('.')[-1].lower()
            if file_ext not in [fmt.replace('.', '') for fmt in SUPPORTED_FORMATS]:
                validation_result['errors'].append(f"Unsupported format: {file_ext}")
                return validation_result
            
            # Read and analyze file
            if file_ext == 'csv':
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
            
            validation_result['rows'] = len(df)
            validation_result['columns'] = list(df.columns)
            validation_result['case_type'] = self.detect_file_type(file_path)
            
            # Case-specific validation
            if validation_result['case_type'] == "CASE_A":
                required_cols = self._find_required_columns_case_a(df.columns)
                if not all(required_cols.values()):
                    validation_result['errors'].append("Missing required columns for Case A: keywords, description, company_name")
            elif validation_result['case_type'] == "CASE_B":
                required_cols = self._find_required_columns_case_b(df.columns)
                if not all(required_cols.values()):
                    validation_result['errors'].append("Missing required columns for Case B: website, company_name")
            else:
                validation_result['errors'].append("Cannot determine processing type - file must have either (keywords + description) or website column")
            
            if not validation_result['errors']:
                validation_result['valid'] = True
            
        except Exception as e:
            validation_result['errors'].append(f"Error reading file: {str(e)}")
            self.logger.error(f"File validation error: {e}")
        
        return validation_result
    
    def _find_required_columns_case_a(self, columns: List[str]) -> Dict[str, Optional[str]]:
        """Find required columns for Case A processing"""
        columns_lower = [col.lower().strip() for col in columns]
        result = {
            'keywords': None,
            'description': None,
            'company_name': None
        }
        
        for i, col in enumerate(columns_lower):
            if 'keyword' in col and not result['keywords']:
                result['keywords'] = columns[i]
            elif ('description' in col or 'desc' in col) and not result['description']:
                result['description'] = columns[i]
            elif ('company' in col or 'name' in col or 'business' in col) and not result['company_name']:
                result['company_name'] = columns[i]
        
        return result
    
    def _find_required_columns_case_b(self, columns: List[str]) -> Dict[str, Optional[str]]:
        """Find required columns for Case B processing"""
        columns_lower = [col.lower().strip() for col in columns]
        result = {
            'website': None,
            'company_name': None
        }
        
        for i, col in enumerate(columns_lower):
            if ('website' in col or 'url' in col or 'web' in col) and not result['website']:
                result['website'] = columns[i]
            elif ('company' in col or 'name' in col or 'business' in col) and not result['company_name']:
                result['company_name'] = columns[i]
        
        return result
    
    def load_data(self, file_path: str) -> Tuple[pd.DataFrame, str]:
        """Load data and return DataFrame with processing type"""
        try:
            # Load file
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
            
            # Detect processing type
            case_type = self.detect_file_type(file_path)
            
            # Clean data
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.reset_index(drop=True)
            
            self.logger.info(f"Loaded {len(df)} rows for {case_type} processing")
            
            return df, case_type
            
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            raise