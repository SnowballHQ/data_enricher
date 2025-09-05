#!/usr/bin/env python3
"""
Data Enrichment System Runner
Simple script to start the Streamlit application
"""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Run the data enrichment system"""
    
    # Get the current directory
    current_dir = Path(__file__).parent
    app_path = current_dir / "app" / "main.py"
    
    # Check if main.py exists
    if not app_path.exists():
        print("❌ Error: app/main.py not found!")
        print("Make sure you're running this from the project root directory.")
        sys.exit(1)
    
    # Check if streamlit is installed
    try:
        import streamlit
    except ImportError:
        print("❌ Error: Streamlit not installed!")
        print("Please run: pip install -r requirements.txt")
        sys.exit(1)
    
    print("🚀 Starting Data Enrichment & Cold Email Personalization System...")
    print("📊 Opening in browser at http://localhost:8501")
    print("Press Ctrl+C to stop the application")
    print("-" * 60)
    
    try:
        # Run streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            str(app_path), "--server.headless", "false"
        ])
    except KeyboardInterrupt:
        print("\n✅ Application stopped successfully!")
    except Exception as e:
        print(f"❌ Error running application: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()