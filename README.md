# 🚀 ETL PIPELINE - COMPLETE DOCUMENTATION

> **A Simple, Production-Ready ETL Pipeline for Extracting, Transforming, and Loading Mixed-Format Data**

---

## 📋 TABLE OF CONTENTS

1. [Project Overview](#project-overview)
2. [What Gets Built](#what-gets-built)
3. [How It Works](#how-it-works)
4. [Presentation to Jury](#presentation-to-jury)
5. [Quick Start Guide](#quick-start-guide)
6. [Detailed Setup](#detailed-setup)
7. [Architecture & Components](#architecture--components)
8. [How to Run](#how-to-run)
9. [Schema & Data Types](#schema--data-types)
10. [Troubleshooting](#troubleshooting)

---

## PROJECT OVERVIEW

### 🎯 What This Project Does

This is a **complete ETL (Extract, Transform, Load) pipeline** that takes messy, unstructured files containing mixed formats (HTML, JSON, plain text, and Base64) and converts them into clean, structured, analyzable data.

**Key Principles:**
- ✅ **No APIs** - Pure Python file processing
- ✅ **No fancy databases** - Just straightforward logic
- ✅ **No complex setup** - One pip command
- ✅ **No backend knowledge needed** - Menu-based CLI
- ✅ **Local storage only** - Privacy-first approach
- ✅ **Works immediately** - Run `python main.py`

### 💡 Real-World Use Cases

1. **Web Scraping Results**: You scrape a website and get HTML, JSON, and text mixed together
2. **API Responses**: Multiple APIs return different formats in one file
3. **Data Migration**: Converting legacy unstructured data to modern CSV/JSON
4. **Log Analysis**: Processing application logs with mixed formats
5. **Data Consolidation**: Merging data from different sources and formats

### 🎓 Perfect For

- Learning ETL concepts
- Portfolio projects
- Data engineering interviews
- Class projects
- Small-scale data processing tasks

---

## WHAT GETS BUILT

### 📁 Project Structure

```
ETL-Pipeline/
│
├── 📄 main.py                        [ENTRY POINT]
│   └─ Interactive menu-based CLI
│   └─ Process files, watch mode, view outputs
│
├── 📄 etl_pipeline.py                [CORE ENGINE]
│   └─ Read files (handles encoding issues)
│   └─ Detect HTML, JSON, text, base64
│   └─ Extract structured data
│   └─ Infer dynamic schema
│   └─ Normalize and clean data
│   └─ Load to CSV, JSON, SQLite
│
├── 📄 app.py                         [WEB INTERFACE - Optional]
│   └─ Flask web server
│   └─ Connects to index.html
│   └─ API endpoints for processing
│
├── 📄 index.html                     [FRONTEND - Optional]
│   └─ Beautiful web UI
│   └─ Drag-and-drop file upload
│   └─ Live results display
│   └─ CSV export button
│
├── 📁 inputs/                        [INPUT FOLDER]
│   └─ Drop your files here to process
│
├── 📁 outputs/                       [OUTPUT FOLDER]
│   ├─ cleaned_output.csv            ← MAIN RESULT (Open in Excel)
│   ├─ dynamic_schema.json           ← Field definitions
│   ├─ processing_metadata.json      ← Statistics and metadata
│   └─ etl_data.db                   ← Optional SQLite database
│
├── 📄 requirement.txt                [DEPENDENCIES]
│   └─ pandas, beautifulsoup4, lxml, watchdog, flask, flask-cors
│
└── 📄 sample_data.txt                [TEST FILE]
    └─ Pre-made test data with mixed formats
```

---

## HOW IT WORKS

### 🏗️ Complete Processing Pipeline

```
YOUR INPUT FILE (mixed formats)
        ↓
   [STEP 1: READ]
   Read file with UTF-8/Latin-1 encoding
        ↓
   [STEP 2: DETECT]
   Find all HTML, JSON, text, base64 blocks
        ↓
   [STEP 3: EXTRACT]
   Pull out structured data from each format
        ↓
   [STEP 4: INFER SCHEMA]
   Discover all unique fields and their types
        ↓
   [STEP 5: NORMALIZE]
   Make all records have same columns
        ↓
   [STEP 6: LOAD]
   Save to CSV, JSON schema, metadata files
        ↓
CLEAN, STRUCTURED OUTPUT (in outputs/ folder)
```

### 📖 Step-by-Step Breakdown

#### **Step 1: Read File**
```python
def read_file(filename: str) -> str:
    # Opens file with proper encoding handling
    # Tries UTF-8 first, falls back to Latin-1
    # Returns raw text content
```
**Why?** Different files have different encodings. Windows often uses Latin-1, Linux uses UTF-8.

#### **Step 2: Detect Content Types**
```python
def detect_content_types(content: str) -> Dict:
    # Looks for <html>, </html> patterns → HTML blocks
    # Looks for { "key": "value" } patterns → JSON
    # Everything else → Plain text
    # Looks for base64 encoded strings
    # Returns categorized content
```

#### **Step 3: Extract Data**
```python
def extract_html(content: str):
    # Parses HTML with BeautifulSoup
    # Extracts text, tables, lists
    # Returns structured records

def extract_json(content: str):
    # Parses JSON strings
    # Validates JSON syntax
    # Returns data objects

def extract_text(content: str):
    # Processes plain text
    # Splits on newlines or delimiters
    # Returns text records
```

#### **Step 4: Infer Schema**
```python
def infer_schema(data: List[Dict]) -> Dict:
    # Looks at all records
    # Finds ALL unique field names
    # Detects data types (string, number, boolean, array)
    # Creates dynamic schema definition
```

#### **Step 5: Normalize Data**
```python
def normalize(extracted_data: List) -> DataFrame:
    # Creates pandas DataFrame
    # Ensures all records have same columns
    # Fills missing values with NaN
    # Standardizes data types
```

#### **Step 6: Load Output**
```python
def load(df: DataFrame, schema: Dict):
    # Saves df to cleaned_output.csv
    # Saves schema to dynamic_schema.json
    # Saves metadata to processing_metadata.json
    # Optional: Stores in SQLite database
```

---

## PRESENTATION TO JURY

### 🎤 How to Present This Project

#### **Opening Statement (30 seconds)**
> "ETL stands for Extract, Transform, Load. This pipeline takes messy, unstructured data from multiple sources with different formats—like HTML from web scraping, JSON from APIs, and plain text from logs—and automatically converts it into clean, structured data that's ready for analysis or storage."

#### **The Problem We're Solving (1 minute)**
- Real-world data is messy and unstructured
- Different sources use different formats
- Manual data cleaning is time-consuming and error-prone
- Excel can't handle complex mixed-format files
- We need automation

#### **Our Solution (2 minutes)**

**Show the folder structure:**
```
📁 inputs/          ← Drop messy file here
📁 outputs/         ← Get clean data here
🐍 main.py          ← Run this
```

**Live Demo Steps:**
1. Show the `inputs/` folder (currently empty or with sample_data.txt)
2. Add a file to `inputs/` or use the web interface
3. Run `python main.py` or `python app.py`
4. Show the menu or web interface
5. Open the resulting `outputs/cleaned_output.csv` in Excel
6. Show the schema in `dynamic_schema.json`

#### **Key Features to Highlight**

| Feature | Why It Matters |
|---------|---|
| **Mixed Format Support** | Handles HTML, JSON, text, base64 all in one file |
| **Automatic Schema Detection** | No need to manually define fields |
| **Data Normalization** | All records structured the same way |
| **Metadata Generation** | Know what was processed and how |
| **Watch Mode** | Auto-process new files as they arrive |
| **Optional Database** | Can store results in SQLite |
| **Local Storage** | Privacy - everything stays on your computer |
| **Simple to Use** | Menu-based CLI, no technical knowledge needed |

#### **Technical Architecture (showing to technical judges)**

```
┌─────────────────────────┐
│    INPUT DATA           │ (HTML + JSON + Text)
│ (formats: mixed)        │
└────────────┬────────────┘
             │
        ┌────▼────┐
        │ EXTRACT  │ ─ Detect content types
        │          │ ─ Parse each format
        └────┬─────┘
             │
        ┌────▼─────────┐
        │ TRANSFORM    │ ─ Infer schema
        │              │ ─ Normalize data
        │              │ ─ Clean values
        └────┬──────────┘
             │
        ┌────▼────┐
        │  LOAD   │ ─ Save to CSV
        │         │ ─ Save schema
        │         │ ─ Save metadata
        └────┬────┘
             │
┌────────────▼─────────────┐
│    OUTPUT DATA          │ (CSV + JSON + Metadata)
│ (format: structured)    │
└─────────────────────────┘
```

#### **Demo Files to Show**

**Before (Input):**
```
inputs/sample_data.txt
- Contains HTML: <h1>Product List</h1>...
- Contains JSON: [{"name": "Laptop", "price": 999}]
- Contains Text: "Some description text"
```

**After (Output):**
```
outputs/cleaned_output.csv
- Row 1: type=json, name=Laptop, price=999, ...
- Row 2: type=json, name=Mouse, price=29, ...
- Row 3: type=text, content=description, ...

outputs/dynamic_schema.json
{
  "type": "string",
  "name": "string", 
  "price": "number",
  ...
}

outputs/processing_metadata.json
{
  "start_time": "2025-11-15T10:30:00",
  "end_time": "2025-11-15T10:30:02",
  "total_items": 3,
  "items_by_type": {"json": 2, "text": 1}
}
```

#### **Why This Matters (Impact Statement)**

- **Business**: Automates repetitive data cleaning tasks
- **Technical**: Demonstrates ETL concepts used in real data pipelines
- **Portfolio**: Shows you understand data processing, Python, file I/O
- **Scalability**: Could be extended to process thousands of files

---

## QUICK START GUIDE

### 3-Minute Setup

```bash
# Step 1: Install dependencies (one time)
cd "d:\ETL Pipeline\ETL-Pipeline"
pip install -r requirement.txt

# Step 2: Add a file to inputs/ folder
# (or use the sample_data.txt provided)

# Step 3: Run the pipeline
python main.py

# Step 4: Follow the menu
# Press 1 to process existing files
# Check outputs/ folder for results
```

### Using the Web Interface

```bash
# Instead of main.py, run the Flask server
python app.py

# Browser opens at http://localhost:5000
# Drag & drop files or paste text
# See results instantly
```

---

## DETAILED SETUP

### System Requirements

- Python 3.8 or higher
- Windows 10/11, macOS, or Linux
- 100MB free disk space
- No internet connection required

### Installation Steps

#### **Step 1: Verify Python Installation**

```bash
python --version
```

Should show Python 3.8 or higher. If not, install from python.org

#### **Step 2: Navigate to Project Folder**

```bash
cd "d:\ETL Pipeline\ETL-Pipeline"
```

#### **Step 3: Install Required Packages**

```bash
pip install -r requirement.txt
```

This installs:
- `pandas` - Data manipulation and CSV handling
- `beautifulsoup4` - HTML/XML parsing
- `lxml` - XML/HTML parsing engine
- `watchdog` - File system monitoring
- `flask` - Web server framework
- `flask-cors` - Cross-origin support

#### **Step 4: Verify Installation**

Run a quick test:
```bash
python -c "import pandas; import bs4; import watchdog; print('✓ All packages installed')"
```

### Folder Structure Auto-Creation

When you run the pipeline for the first time:
- `inputs/` folder is created automatically
- `outputs/` folder is created automatically
- Both folders have proper permissions

---

## ARCHITECTURE & COMPONENTS

### Component 1: main.py (CLI Entry Point)

**Purpose**: Provides interactive menu interface

**Features**:
- Menu-based user interface
- Option to process existing files
- Watch mode for continuous processing
- Display results
- Database option

**Code Flow**:
```
user selects option 1
    ↓
SimpleETL.process_file() called
    ↓
ETLPipeline.run() executes
    ↓
Results saved to outputs/
    ↓
Success message displayed
```

### Component 2: etl_pipeline.py (Core Engine)

**Purpose**: Implements complete ETL logic

**Key Methods**:

| Method | Input | Output |
|--------|-------|--------|
| `read_file(filename)` | File path | Raw text content |
| `detect_content_types(content)` | Raw text | Dict of {html, json, text, base64} |
| `extract_html(html_content)` | HTML string | List of records |
| `extract_json(json_content)` | JSON string | List of records |
| `extract_text(text_content)` | Text string | List of records |
| `infer_schema(data)` | List of records | Schema dict |
| `normalize(extracted_data)` | Mixed records | Pandas DataFrame |
| `load(df, schema)` | DataFrame + schema | Saves files |
| `run(filename)` | Input filename | Returns df, schema |

### Component 3: app.py (Web Server)

**Purpose**: Provides Flask REST API and web interface

**API Endpoints**:
- `GET /` - Serves index.html frontend
- `POST /process` - Processes uploaded data
- `GET /diagnostic.html` - Diagnostic test page
- `GET /console_test.html` - Console testing page

**Features**:
- File upload handling
- Real-time processing
- JSON response formatting
- Error handling

### Component 4: index.html (Web Frontend)

**Purpose**: Beautiful user interface

**Features**:
- Drag-and-drop file upload
- Paste text directly
- Live data display
- Schema visualization
- CSV export button
- Sample data buttons

---

## HOW TO RUN

### Option 1: Command-Line Interface (Recommended for Jury)

**Most straightforward, no web server needed**

```bash
python main.py
```

Menu will appear:
```
🚀 SIMPLE ETL PIPELINE

Options:
1. Process existing files in inputs/ folder
2. Watch inputs/ folder for new files (auto-process)
3. Process a specific file
4. View outputs
5. Exit

Enter your choice: _
```

**Workflow**:
1. Put a file in `inputs/` folder
2. Select option 1
3. Pipeline processes the file
4. Results appear in `outputs/` folder

### Option 2: Web Interface (Best for Demo)

**Beautiful UI, good for presentations**

```bash
python app.py
```

Browser automatically opens to `http://localhost:5000`

**Features**:
- Drag & drop file upload
- Paste text directly
- Live results
- Professional UI
- CSV export button

### Option 3: Watch Mode (Auto-Processing)

**Automatically process new files as they're added**

```bash
python main.py watch
```

Then add files to `inputs/` - they'll be processed automatically!

### Option 4: Process with Database

**Store results in SQLite database**

```bash
python main.py db
```

Results saved to `outputs/etl_data.db` in addition to CSV/JSON

---

## SCHEMA & DATA TYPES

### Understanding Schema Output

The schema is **dynamically generated** based on your input data. Different input types produce different numbers of fields.

### Universal Fields (Always Present)

Every output always includes these 3 fields:

```json
{
  "type": "string",              // Format of record (html, json, text)
  "source_index": "string",      // Unique ID (html_0, json_1, etc)
  "total_items": "number"        // Total records processed
}
```

### Data-Specific Fields

These fields depend on your input:

#### JSON Input → Multiple Fields
```json
[
  {
    "product_name": "Laptop",
    "price": 1299.99,
    "in_stock": true
  }
]
```

**Resulting Schema**:
```json
{
  "type": "string",
  "source_index": "string",
  "total_items": "number",
  "product_name": "string",
  "price": "number",
  "in_stock": "boolean"
}
```

#### HTML Input → 3 Fields
```html
<html>
  <h1>Welcome</h1>
  <p>Content here</p>
</html>
```

**Resulting Schema**:
```json
{
  "type": "string",
  "source_index": "string", 
  "total_items": "number"
}
```
*Note: HTML parsing doesn't extract specific fields - only structural info*

#### Plain Text Input → 3 Fields
```
This is just plain text.
No structured data here.
```

**Resulting Schema**:
```json
{
  "type": "string",
  "source_index": "string",
  "total_items": "number"
}
```

### Data Type Detection

| Type | Python | JSON | Example |
|------|--------|------|---------|
| String | `str` | `string` | `"Hello"`, `"123"` |
| Number | `int`, `float` | `number` | `42`, `3.14` |
| Boolean | `bool` | `boolean` | `True`, `False` |
| Array | `list` | `array` | `["a", "b"]` |
| DateTime | `datetime` | `datetime` | `2025-11-15T10:30:00Z` |
| Null | `None` | `null` | Missing value |

### Output Files Explained

#### 1. cleaned_output.csv
```
type,source_index,total_items,product_name,price,in_stock
json,json_0,2,Laptop,1299.99,True
json,json_1,2,Mouse,29.99,True
text,text_0,1,,,,
html,html_0,1,,,,
```

- Open in Excel for easy viewing
- All records have same columns
- Missing fields shown as empty

#### 2. dynamic_schema.json
```json
{
  "type": "string",
  "source_index": "string",
  "total_items": "number",
  "product_name": "string",
  "price": "number",
  "in_stock": "boolean"
}
```

- Defines all fields and their types
- Useful for data validation
- Can be used in other tools

#### 3. processing_metadata.json
```json
{
  "start_time": "2025-11-15T10:30:00.123456",
  "end_time": "2025-11-15T10:30:02.456789",
  "filename": "input_data.txt",
  "total_items": 3,
  "items_by_type": {
    "json": 2,
    "html": 1,
    "text": 0
  },
  "processing_duration_seconds": 2.333
}
```

- Shows what was processed
- Timestamps of execution
- Item counts by type
- Processing duration

---

## TROUBLESHOOTING

### Common Issues & Solutions

#### Issue 1: "pip command not found"

**Symptom**: 
```
'pip' is not recognized as an internal or external command
```

**Solution**:
```bash
# Try with Python module invocation
python -m pip install -r requirement.txt
```

Or install Python from python.org (check "Add Python to PATH")

#### Issue 2: "File not found in inputs/ folder"

**Symptom**:
```
FileNotFoundError: inputs/myfile.txt
```

**Solution**:
1. Verify file is actually in `inputs/` folder
2. Check filename spelling and case
3. File must have an extension (.txt, .html, .json, etc)

#### Issue 3: "Encoding error - UnicodeDecodeError"

**Symptom**:
```
UnicodeDecodeError: 'utf-8' codec can't decode byte...
```

**Solution**:
- Pipeline automatically tries UTF-8 then Latin-1
- If still fails, convert file to UTF-8:
  - In Notepad++: Encoding → Encode in UTF-8
  - In VSCode: Save with encoding → UTF-8

#### Issue 4: "Port 5000 already in use" (Flask)

**Symptom**:
```
OSError: [Errno 48] Address already in use
```

**Solution**:
```bash
# Kill existing Flask server
# Windows:
netstat -ano | findstr :5000
taskkill /PID <PID> /F

# Or run on different port:
python app.py --port 5001
```

#### Issue 5: "Empty output - no data extracted"

**Symptom**: 
- CSV is created but empty (only headers)
- Schema shows only 3 fields

**Solution**:
1. Check input file format - ensure it contains valid HTML/JSON/Text
2. Use sample_data.txt to test
3. Verify file isn't binary
4. Check file encoding

#### Issue 6: "JSON parsing error"

**Symptom**:
```
JSONDecodeError: Expecting value...
```

**Solution**:
- Ensure JSON is valid (use jsonlint.com to validate)
- Remove comments (JSON doesn't allow comments)
- JSON should be arrays or objects: `[{...}]` or `{...}`

### Getting Help

**Check these files**:
1. `outputs/processing_metadata.json` - Shows what was processed
2. `debug.log` - Contains detailed error messages
3. `server.log` - Contains Flask server logs

**Test with sample data**:
```bash
# Use the provided test file
# It contains valid mixed-format data
```

---

## TESTING & VALIDATION

### Test the Pipeline

**Test 1: Simple JSON**
```bash
python main.py
# Put sample_data.txt in inputs/
# Select option 1
# Check outputs/cleaned_output.csv
```

**Test 2: Watch Mode**
```bash
python main.py watch
# Drop a file into inputs/ folder
# It processes automatically
```

**Test 3: Web Interface**
```bash
python app.py
# Opens browser at http://localhost:5000
# Drag & drop a file
# See results instantly
```

### Validation Checklist

- ✅ Python version 3.8+
- ✅ All packages installed (`pip install -r requirement.txt`)
- ✅ `inputs/` folder exists
- ✅ `outputs/` folder exists
- ✅ Input file is readable (not corrupted)
- ✅ Input file has valid data
- ✅ Output files created (CSV, JSON, metadata)

---

## KEY FEATURES SUMMARY

| Feature | Description | Use Case |
|---------|-------------|----------|
| **Mixed Format Support** | Handles HTML, JSON, text, base64 in one file | Web scraping results |
| **Auto Schema Detection** | Automatically finds all fields and types | No manual config needed |
| **Data Normalization** | All records structured identically | Ready for analysis |
| **Local Processing** | No cloud, everything local | Privacy & security |
| **Watch Mode** | Auto-process new files | Batch processing |
| **Optional Database** | Save to SQLite if needed | Data persistence |
| **Metadata** | Tracking and statistics | Audit trail |
| **Simple CLI** | Menu-based, beginner-friendly | Easy to use |
| **Web Interface** | Beautiful UI available | Professional demos |

---

## FILE PROCESSING EXAMPLES

### Example 1: Processing Mixed Web Scrape

**Input** (inputs/scraped_data.txt):
```
<html>
  <div class="product">
    <h1>Laptop</h1>
    <p>Price: $999</p>
  </div>
</html>

{
  "product": "Mouse",
  "price": 29.99,
  "stock": true
}
```

**Output** (outputs/cleaned_output.csv):
```
type,source_index,total_items,product,price,stock
html,html_0,2,,,
json,json_0,2,Mouse,29.99,True
```

### Example 2: Processing JSON Array

**Input** (inputs/users.txt):
```json
[
  {"id": 1, "name": "Alice", "email": "alice@example.com"},
  {"id": 2, "name": "Bob", "email": "bob@example.com"}
]
```

**Output** (outputs/cleaned_output.csv):
```
type,source_index,total_items,id,name,email
json,json_0,2,1,Alice,alice@example.com
json,json_1,2,2,Bob,bob@example.com
```

---

## PERFORMANCE CHARACTERISTICS

- **Small files** (< 1MB): Process in < 1 second
- **Medium files** (1-10MB): Process in 1-5 seconds
- **Large files** (10-100MB): Process in 5-30 seconds
- **Very large files** (>100MB): Consider splitting into chunks

---

## LIMITATIONS & KNOWN ISSUES

1. **Large File Memory**: Very large files (>500MB) may consume significant RAM
2. **Complex HTML**: Nested HTML tables may not parse perfectly
3. **Binary Files**: Cannot process binary files (images, PDFs, etc.)
4. **Encoding**: Uses UTF-8/Latin-1; other encodings may fail
5. **Database**: SQLite has 2GB file size limit

---

## NEXT STEPS & ENHANCEMENTS

Potential improvements:
- Add database storage for multiple files
- Implement data validation rules
- Add column filtering/selection
- Support for CSV, XML input formats
- Cloud storage integration (optional)
- Scheduled processing
- Error alerts/logging

---

## CONCLUSION

This ETL pipeline demonstrates:
- ✅ Core ETL concepts (Extract, Transform, Load)
- ✅ Python file I/O and data processing
- ✅ Error handling and encoding management
- ✅ Data schema inference
- ✅ Pandas DataFrame operations
- ✅ Flask REST API development
- ✅ User interface design
- ✅ Real-world data problems

Perfect for portfolios, interviews, or learning data engineering!

---

## QUICK REFERENCE COMMANDS

```bash
# Installation
pip install -r requirement.txt

# CLI mode
python main.py

# Watch mode
python main.py watch

# Web interface
python app.py

# Test
python -c "import pandas, bs4, watchdog; print('OK')"

# Process specific file (from CLI menu)
# Select option 3 and enter filename
```

---

**Last Updated**: November 15, 2025  
**Status**: Production Ready ✅  
**Version**: 1.0  
**Author**: Your Name

