# Data Enrichment & Cold Email Personalization System

A comprehensive system for enriching company data and generating personalized cold email snippets.

## 🚀 Features

- **Dual Processing Modes:**
  - **Case A:** Direct processing of files with keywords and description columns
  - **Case B:** Automatic web scraping for files with website URLs only

- **AI-Powered Category Extraction:** Uses OpenAI API and keyword matching
- **Personalized Email Generation:** Creates targeted cold email snippets
- **Scalable Architecture:** Handles large datasets efficiently
- **Web Interface:** User-friendly Streamlit interface
- **Multiple Export Formats:** Excel and CSV downloads

## 📁 Project Structure

```
data-enrichment-system/
├── app/
│   ├── main.py                 # Streamlit main application
│   ├── file_processor.py       # File detection and validation
│   ├── case_a_processor.py     # Keywords/description processing
│   ├── case_b_processor.py     # Website scraping processing
│   └── utils/
│       ├── email_generator.py
│       └── __init__.py
├── scrapy_project/
│   ├── spiders/
│   │   └── website_spider.py   # Web scraping spider
│   ├── items.py
│   ├── pipelines.py
│   ├── settings.py
│   └── scrapy.cfg
├── config/
│   └── settings.py            # Application configuration
├── uploads/                    # Temporary file uploads
├── outputs/                    # Processed file outputs
├── logs/                       # Application logs
├── requirements.txt
├── .env.example
└── README.md
```

## 🛠️ Installation

1. **Clone or download the project:**
   ```bash
   cd data-enrichment-system
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env and add your OpenAI API key
   ```

4. **Create necessary directories:**
   ```bash
   mkdir -p logs uploads outputs
   ```

## 🚀 Usage

### Starting the Application

```bash
streamlit run app/main.py
```

The application will be available at `http://localhost:8501`

### Processing Files

1. **Upload File:** Choose CSV or Excel file (max 50MB)
2. **Auto-Detection:** System detects Case A or Case B automatically
3. **Processing:** Click "Start Processing" to begin
4. **Download:** Get enriched file in Excel or CSV format

### Input File Requirements

**Case A (Keywords + Description):**
- Required columns: `keywords`, `description`, `company_name`
- Example:
  ```
  company_name,keywords,description
  TechCorp,software saas platform,Cloud-based project management software
  ```

**Case B (Website Only):**
- Required columns: `website`, `company_name`
- Example:
  ```
  company_name,website
  TechCorp,https://techcorp.com
  ```

### Output Format

Both cases generate:
- **category:** Extracted product category
- **email_snippet:** Personalized cold email snippet with placeholder

Example snippet:
```
"When we asked ChatGPT about saas solutions brands [brands like yours], 
it listed several competitors, but {{companyName}} didn't appear."
```

## ⚙️ Configuration

### AI-Powered Categorization

The system now uses OpenAI AI to generate categories dynamically:

- **No predefined categories** - AI analyzes each product individually
- **Industry-specific categorization** - AI considers business context and sector
- **Professional standards** - Categories follow business classification best practices
- **Dynamic generation** - Each product gets a tailored, accurate category

### Scraping Settings

Modify `config/settings.py` for scraping configuration:

```python
PAGES_TO_SCRAPE = ['/', '/about', '/products', '/services']
SCRAPY_SETTINGS = {
    'DOWNLOAD_DELAY': 1,
    'CONCURRENT_REQUESTS': 16
}
```

## 🔧 API Configuration

### OpenAI API

1. Get API key from [OpenAI](https://platform.openai.com/api-keys)
2. Add to `.env` file:
   ```
   OPENAI_API_KEY=your_api_key_here
   ```

### OpenAI API Required

The system now requires OpenAI API for categorization - no fallback methods available.

## 📊 Performance

- **Case A:** ~1000 rows/hour
- **Case B:** ~500 rows/hour (depends on website response time)
- **Memory Usage:** Processes files in chunks to handle large datasets
- **Parallel Processing:** Configurable worker threads

## 🛡️ Error Handling

- **File Validation:** Checks format, size, and required columns
- **URL Validation:** Validates website URLs before scraping
- **Retry Logic:** Automatic retries for failed requests
- **Graceful Degradation:** Continues processing if some rows fail

## 📝 Logging

Logs are stored in `logs/app.log` with configurable levels:
- Processing status
- Error details
- Performance metrics

## 🔍 Troubleshooting

### Common Issues

1. **Scrapy not found:**
   ```bash
   pip install scrapy
   ```

2. **OpenAI API errors:**
   - Check API key validity
   - Verify rate limits
   - Check account balance

3. **Large file processing:**
   - Increase chunk size in settings
   - Use parallel processing
   - Monitor memory usage

### Debug Mode

Enable debug logging in `.env`:
```
LOG_LEVEL=DEBUG
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Make changes
4. Test thoroughly
5. Submit pull request

## 📄 License

This project is for educational and commercial use. Ensure compliance with website terms of service when scraping.

## 🆘 Support

For issues and questions:
1. Check the logs in `logs/app.log`
2. Review configuration files
3. Verify dependencies installation
4. Test with sample data

---

**Note:** This system is designed to safely process data without disturbing existing functions. All processing is self-contained within the project directory.