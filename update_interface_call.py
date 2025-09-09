#!/usr/bin/env python3
"""
Script to update interface to pass processing_mode to processor
"""

# Read the interface file
with open('utils/google_sheets_interface_persistent.py', 'r') as f:
    content = f.read()

# Find and update the processor call
old_call = '''result = processor.process_sheet_range(
                                sheet_id, start_row, num_rows,
                                progress_callback=update_progress,
                                sheet_name=sheet_name
                            )'''

new_call = '''result = processor.process_sheet_range(
                                sheet_id, start_row, num_rows,
                                progress_callback=update_progress,
                                sheet_name=sheet_name,
                                processing_mode=processing_mode
                            )'''

# Apply the change
content = content.replace(old_call, new_call)

# Write back to file
with open('utils/google_sheets_interface_persistent.py', 'w') as f:
    f.write(content)

print("✅ Updated interface to pass processing_mode to processor")
print("✅ Manual toggle now controls case selection")
