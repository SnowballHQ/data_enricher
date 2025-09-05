@echo off
echo ========================================
echo   Product Categorization System
echo   Case A: Keywords + Description
echo   Case B: Website URLs + Scraping
echo ========================================
echo.

echo Checking setup...
python setup.py

echo.
echo ========================================
echo Starting the application...
echo ========================================
echo.
echo The app will open in your browser at:
echo http://localhost:8501
echo.
echo Press Ctrl+C to stop the application
echo.

streamlit run app.py

pause
