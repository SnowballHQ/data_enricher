#!/usr/bin/env python3
"""
Script to update Google Sheets processor to use manual toggle
"""

# Read the processor file
with open('utils/google_sheets_processor_fixed.py', 'r') as f:
    content = f.read()

# Update the process_sheet_range method signature to accept processing_mode
old_signature = '''def process_sheet_range(self, sheet_id: str, start_row: int, num_rows: int, 
                       progress_callback=None, sheet_name: str = "Sheet1") -> Dict:'''

new_signature = '''def process_sheet_range(self, sheet_id: str, start_row: int, num_rows: int, 
                       progress_callback=None, sheet_name: str = "Sheet1", 
                       processing_mode: str = "auto") -> Dict:'''

# Update the case detection logic
old_detection = '''            # Detect processing case
            case_type = self._detect_processing_case(column_mapping)
            
            # Validate required columns based on case type
            missing_cols = []
            if case_type == "CASE_A":
                if 'keywords' not in column_mapping:
                    missing_cols.append('keywords')
                if 'description' not in column_mapping:
                    missing_cols.append('description')
            elif case_type == "CASE_B":
                if 'website' not in column_mapping:
                    missing_cols.append('website')
                # company_name is optional for Case B
            else:
                return {"error": "Could not detect valid data format. Please ensure your sheet has either:\\n- Case A: 'Company Keywords' + 'Company Short Description'\\n- Case B: 'Website' + 'Company Name' (optional)"}'''

new_detection = '''            # Use manual processing mode or auto-detect
            if processing_mode == "Case A: Keywords + Description":
                case_type = "CASE_A"
            elif processing_mode == "Case B: Website + Company":
                case_type = "CASE_B"
            else:
                # Auto-detect (fallback)
                case_type = self._detect_processing_case(column_mapping)
            
            # Validate required columns based on case type
            missing_cols = []
            if case_type == "CASE_A":
                if 'keywords' not in column_mapping:
                    missing_cols.append('Company Keywords')
                if 'description' not in column_mapping:
                    missing_cols.append('Company Short Description')
            elif case_type == "CASE_B":
                if 'website' not in column_mapping:
                    missing_cols.append('Website')
                # company_name is optional for Case B
            else:
                return {"error": "Could not detect valid data format. Please ensure your sheet has either:\\n- Case A: 'Company Keywords' + 'Company Short Description'\\n- Case B: 'Website' + 'Company Name' (optional)"}'''

# Apply the changes
content = content.replace(old_signature, new_signature)
content = content.replace(old_detection, new_detection)

# Write back to file
with open('utils/google_sheets_processor_fixed.py', 'w') as f:
    f.write(content)

print("✅ Updated process_sheet_range to accept processing_mode parameter")
print("✅ Modified case detection to use manual toggle")
print("✅ Updated validation messages for better user guidance")
