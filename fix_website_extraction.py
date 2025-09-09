#!/usr/bin/env python3
"""
Script to fix the website field extraction in google_sheets_processor_fixed.py
"""

# Read the current file
with open('utils/google_sheets_processor_fixed.py', 'r') as f:
    content = f.read()

# Find and replace the row_data initialization
old_row_data = '''                row_data = {
                    'row_number': data_start_row + i,  # Track actual row number
                    'keywords': '',
                    'description': '',
                    'company_name': ''
                }'''

new_row_data = '''                row_data = {
                    'row_number': data_start_row + i,  # Track actual row number
                    'keywords': '',
                    'description': '',
                    'company_name': '',
                    'website': ''
                }'''

# Find and replace the company_name extraction section
old_extraction = '''                if 'company_name' in column_mapping:
                    col_index = ord(column_mapping['company_name']) - ord('A')
                    if col_index < len(row):
                        row_data['company_name'] = row[col_index] if row[col_index] else ''
                
                mapped_data.append(row_data)'''

new_extraction = '''                if 'company_name' in column_mapping:
                    col_index = ord(column_mapping['company_name']) - ord('A')
                    if col_index < len(row):
                        row_data['company_name'] = row[col_index] if row[col_index] else ''
                
                if 'website' in column_mapping:
                    col_index = ord(column_mapping['website']) - ord('A')
                    if col_index < len(row):
                        row_data['website'] = row[col_index] if row[col_index] else ''
                
                mapped_data.append(row_data)'''

# Apply the fixes
content = content.replace(old_row_data, new_row_data)
content = content.replace(old_extraction, new_extraction)

# Write back to file
with open('utils/google_sheets_processor_fixed.py', 'w') as f:
    f.write(content)

print("✅ Fixed website field extraction in google_sheets_processor_fixed.py")
print("✅ Added 'website': '' to row_data initialization")
print("✅ Added website column extraction logic")
