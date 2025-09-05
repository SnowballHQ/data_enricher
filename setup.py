#!/usr/bin/env python3
"""
Setup script for Product Categorization System
This script helps initialize the configuration and create necessary files
"""

import os
import sys
from pathlib import Path

def main():
    print("🚀 Setting up Product Categorization System...")
    print("=" * 50)
    
    # Check if Python dependencies are installed
    try:
        import streamlit
        import pandas
        import openai
        print("✅ All required Python packages are installed")
    except ImportError as e:
        print(f"❌ Missing package: {e}")
        print("Please run: pip install -r requirements.txt")
        return
    
    # Create necessary directories
    Path("models").mkdir(exist_ok=True)
    Path("utils").mkdir(exist_ok=True)
    print("✅ Directory structure created")
    
    # Check if config.json exists
    if Path("config.json").exists():
        print("✅ Configuration file already exists")
    else:
        print("ℹ️  Configuration file will be created on first run")
    
    # Create .env template
    env_template = """# OpenAI API Configuration
# Copy this file to .env and replace with your actual values
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-5-nano
OPENAI_MAX_TOKENS=100
OPENAI_TEMPERATURE=0.1
"""
    
    try:
        with open("env_template.txt", "w") as f:
            f.write(env_template)
        print("✅ Environment template created (env_template.txt)")
    except Exception as e:
        print(f"❌ Error creating env template: {e}")
    
    print("\n" + "=" * 50)
    print("🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Get your OpenAI API key from https://platform.openai.com/api-keys")
    print("2. Run: streamlit run app.py")
    print("3. Enter your API key in the app (it will be saved automatically)")
    print("4. Upload your data file and start processing!")
    print("\nFor help, check the README.md file")

if __name__ == "__main__":
    main()
