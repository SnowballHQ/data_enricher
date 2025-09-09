
#!/usr/bin/env python3
"""
Script to add manual Case A/B toggle to Google Sheets interface
"""

# Read the current file
with open('utils/google_sheets_interface_persistent.py', 'r') as f:
    content = f.read()

# Find the insertion point (after num_rows definition)
insertion_point = '''        num_rows = st.number_input(
            "Rows to Process",
            min_value=1,
            value=10,
            max_value=1000,
            help="Number of rows to process"
        )
    
    # Validation and preview'''

# New manual toggle section
toggle_section = '''        num_rows = st.number_input(
            "Rows to Process",
            min_value=1,
            value=10,
            max_value=1000,
            help="Number of rows to process"
        )
    
    # Data Type Selection
    st.subheader("🔄 Data Type Selection")
    processing_mode = st.radio(
        "Select your data format:",
        options=["Case A: Keywords + Description", "Case B: Website + Company"],
        help="Choose based on your sheet columns",
        horizontal=True
    )
    
    # Show info based on selection
    if processing_mode == "Case A: Keywords + Description":
        st.info("📋 **Required columns:** 'Company Keywords' + 'Company Short Description'")
    else:
        st.info("🌐 **Required columns:** 'Website' + 'Company Name' (optional)")
    
    # Validation and preview'''

# Apply the replacement
content = content.replace(insertion_point, toggle_section)

# Write back to file
with open('utils/google_sheets_interface_persistent.py', 'w') as f:
    f.write(content)

print("✅ Added manual Case A/B toggle to Google Sheets interface")
print("✅ Added data type selection radio buttons")
print("✅ Added helpful info messages for each case")
