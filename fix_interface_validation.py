#!/usr/bin/env python3
"""
Script to fix interface validation to respect manual toggle
"""

# Read the interface file
with open('utils/google_sheets_interface_persistent.py', 'r') as f:
    content = f.read()

# Find and replace the hardcoded validation
old_validation = '''                            # Show processing readiness
                            missing_cols = []
                            if 'keywords' not in mapping:
                                missing_cols.append('keywords')
                            if 'description' not in mapping:
                                missing_cols.append('description')
                            
                            if missing_cols:
                                st.error(f"❌ Cannot process: Missing required columns: {', '.join(missing_cols)}")
                            else:
                                st.success("✅ Ready to process! All required columns detected.")'''

new_validation = '''                            # Show processing readiness based on selected mode
                            missing_cols = []
                            if processing_mode == "Case A: Keywords + Description":
                                if 'keywords' not in mapping:
                                    missing_cols.append('Company Keywords')
                                if 'description' not in mapping:
                                    missing_cols.append('Company Short Description')
                            elif processing_mode == "Case B: Website + Company":
                                if 'website' not in mapping:
                                    missing_cols.append('Website')
                                # company_name is optional for Case B
                            
                            if missing_cols:
                                st.error(f"❌ Cannot process: Missing required columns: {', '.join(missing_cols)}")
                            else:
                                st.success("✅ Ready to process! All required columns detected.")'''

# Apply the fix
content = content.replace(old_validation, new_validation)

# Also fix any other hardcoded validation
old_validation2 = '''                        missing_cols = []
                        if 'keywords' not in mapping:
                            missing_cols.append('keywords')
                        if 'description' not in mapping:
                            missing_cols.append('description')
                        
                        if missing_cols:
                            st.error(f"❌ Cannot process: Missing required columns: {', '.join(missing_cols)}")'''

new_validation2 = '''                        missing_cols = []
                        if processing_mode == "Case A: Keywords + Description":
                            if 'keywords' not in mapping:
                                missing_cols.append('Company Keywords')
                            if 'description' not in mapping:
                                missing_cols.append('Company Short Description')
                        elif processing_mode == "Case B: Website + Company":
                            if 'website' not in mapping:
                                missing_cols.append('Website')
                            # company_name is optional for Case B
                        
                        if missing_cols:
                            st.error(f"❌ Cannot process: Missing required columns: {', '.join(missing_cols)}")'''

content = content.replace(old_validation2, new_validation2)

# Write back to file
with open('utils/google_sheets_interface_persistent.py', 'w') as f:
    f.write(content)

print("✅ Fixed interface validation to respect manual toggle")
print("✅ Case A requires: Company Keywords + Company Short Description")
print("✅ Case B requires: Website (Company Name optional)")
