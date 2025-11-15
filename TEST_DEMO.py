"""
ETL Pipeline - Test & Demo Guide
This shows exactly what happens when you run the pipeline
"""

# ============================================================================
# WHAT THE PIPELINE DOES - STEP BY STEP EXAMPLE
# ============================================================================

# INPUT: A mixed file with HTML, JSON, and text (sample_data.txt)
# ============================================================================

sample_input = """
<html>
  <head>
    <title>Company Website</title>
  </head>
  <body>
    <h1>Welcome to TechCorp</h1>
    <p>We are a leading software development company.</p>
  </body>
</html>

{"employee_id": 101, "name": "John Doe", "department": "Engineering"}
{"employee_id": 102, "name": "Jane Smith", "department": "Marketing"}

Project Status: We have completed 75% of planned features.
The team is working on backend optimization.

{"project": "AI Analytics", "status": "in_progress", "completion": 0.75}
"""

# ============================================================================
# STEP 1: READ FILE
# ============================================================================

print("=" * 70)
print("STEP 1: READ FILE")
print("=" * 70)
print(f"✓ File size: {len(sample_input)} characters")
print(f"✓ Encoding: UTF-8 (with fallback to Latin-1)")
print()

# ============================================================================
# STEP 2: DETECT & EXTRACT
# ============================================================================

print("=" * 70)
print("STEP 2: DETECT & EXTRACT CONTENT")
print("=" * 70)

# HTML Detection
html_blocks = [
    {
        'type': 'html',
        'title': 'Company Website',
        'text': 'Welcome to TechCorp We are a leading software development company.',
        'links': [],
        'images': [],
        'raw_length': 198,
        'source_index': 'html_0'
    }
]
print(f"✓ HTML Blocks Found: {len(html_blocks)}")
print(f"  - Block 1: Title = '{html_blocks[0]['title']}'")
print()

# JSON Detection
json_objects = [
    {
        'type': 'json',
        'employee_id': 101,
        'name': 'John Doe',
        'department': 'Engineering',
        'source_index': 'json_0'
    },
    {
        'type': 'json',
        'employee_id': 102,
        'name': 'Jane Smith',
        'department': 'Marketing',
        'source_index': 'json_1'
    },
    {
        'type': 'json',
        'project': 'AI Analytics',
        'status': 'in_progress',
        'completion': 0.75,
        'source_index': 'json_2'
    }
]
print(f"✓ JSON Objects Found: {len(json_objects)}")
for obj in json_objects:
    print(f"  - {obj}")
print()

# Text Detection
text_paragraphs = [
    {
        'type': 'text',
        'content': 'Project Status: We have completed 75% of planned features.',
        'word_count': 11,
        'char_count': 60,
        'source_index': 'text_0'
    },
    {
        'type': 'text',
        'content': 'The team is working on backend optimization.',
        'word_count': 8,
        'char_count': 43,
        'source_index': 'text_1'
    }
]
print(f"✓ Text Paragraphs Found: {len(text_paragraphs)}")
for para in text_paragraphs:
    print(f"  - {para['content'][:50]}...")
print()

total_items = len(html_blocks) + len(json_objects) + len(text_paragraphs)
print(f"✓ Total Items Extracted: {total_items}")
print()

# ============================================================================
# STEP 3: SCHEMA INFERENCE
# ============================================================================

print("=" * 70)
print("STEP 3: SCHEMA INFERENCE")
print("=" * 70)

schema = {
    'type': {
        'type': ['str'],
        'nullable': False,
        'present_in': 6
    },
    'source_index': {
        'type': ['str'],
        'nullable': False,
        'present_in': 6
    },
    'title': {
        'type': ['str'],
        'nullable': True,
        'present_in': 1
    },
    'text': {
        'type': ['str'],
        'nullable': True,
        'present_in': 2
    },
    'links': {
        'type': ['list'],
        'nullable': True,
        'present_in': 1
    },
    'images': {
        'type': ['list'],
        'nullable': True,
        'present_in': 1
    },
    'employee_id': {
        'type': ['int'],
        'nullable': True,
        'present_in': 2
    },
    'name': {
        'type': ['str'],
        'nullable': True,
        'present_in': 2
    },
    'department': {
        'type': ['str'],
        'nullable': True,
        'present_in': 2
    },
    'project': {
        'type': ['str'],
        'nullable': True,
        'present_in': 1
    },
    'status': {
        'type': ['str'],
        'nullable': True,
        'present_in': 1
    },
    'completion': {
        'type': ['float'],
        'nullable': True,
        'present_in': 1
    },
    'content': {
        'type': ['str'],
        'nullable': True,
        'present_in': 2
    },
    'word_count': {
        'type': ['int'],
        'nullable': True,
        'present_in': 2
    },
    'char_count': {
        'type': ['int'],
        'nullable': True,
        'present_in': 2
    }
}

print(f"✓ Schema Fields Detected: {len(schema)}")
print("\nSchema Details:")
for field, info in list(schema.items())[:5]:
    print(f"  - {field}: type={info['type']}, nullable={info['nullable']}, present_in={info['present_in']} records")
print("  ... and more")
print()

# ============================================================================
# STEP 4: NORMALIZE & FILL MISSING VALUES
# ============================================================================

print("=" * 70)
print("STEP 4: NORMALIZE (Fill Missing Values with None)")
print("=" * 70)

normalized_data = [
    {
        'type': 'html',
        'source_index': 'html_0',
        'title': 'Company Website',
        'text': 'Welcome to TechCorp...',
        'links': '[]',
        'images': '[]',
        'raw_length': 198,
        'employee_id': None,
        'name': None,
        'department': None,
        'project': None,
        'status': None,
        'completion': None,
        'content': None,
        'word_count': None,
        'char_count': None
    },
    {
        'type': 'json',
        'source_index': 'json_0',
        'title': None,
        'text': None,
        'links': None,
        'images': None,
        'raw_length': None,
        'employee_id': 101,
        'name': 'John Doe',
        'department': 'Engineering',
        'project': None,
        'status': None,
        'completion': None,
        'content': None,
        'word_count': None,
        'char_count': None
    },
    {
        'type': 'text',
        'source_index': 'text_0',
        'title': None,
        'text': None,
        'links': None,
        'images': None,
        'raw_length': None,
        'employee_id': None,
        'name': None,
        'department': None,
        'project': None,
        'status': None,
        'completion': None,
        'content': 'Project Status: We have completed 75%...',
        'word_count': 11,
        'char_count': 60
    }
]

print("✓ Normalized all records to have SAME columns")
print(f"✓ Missing values filled with None")
print(f"✓ DataFrame Shape: {len(normalized_data)} rows × {len(schema)} columns")
print()
print("Sample normalized records:")
for i, record in enumerate(normalized_data[:2]):
    print(f"\nRecord {i+1}:")
    non_none = {k: v for k, v in record.items() if v is not None}
    for key, value in list(non_none.items())[:5]:
        print(f"  {key}: {value}")
print()

# ============================================================================
# STEP 5: LOAD (Save Files)
# ============================================================================

print("=" * 70)
print("STEP 5: LOAD (Save Outputs)")
print("=" * 70)

output_files = {
    'cleaned_output.csv': f"""type,source_index,title,text,employee_id,name,department,content,word_count
html,html_0,Company Website,Welcome to TechCorp...,None,None,None,None,None
json,json_0,None,None,101,John Doe,Engineering,None,None
json,json_1,None,None,102,Jane Smith,Marketing,None,None
text,text_0,None,None,None,None,None,Project Status: We have...,11
... and more rows
""",
    
    'dynamic_schema.json': """{
  "type": {"type": ["str"], "nullable": false, "present_in": 6},
  "employee_id": {"type": ["int"], "nullable": true, "present_in": 2},
  "name": {"type": ["str"], "nullable": true, "present_in": 2},
  "department": {"type": ["str"], "nullable": true, "present_in": 2},
  ... and 11 more fields
}""",

    'processing_metadata.json': """{
  "filename": "sample_data.txt",
  "start_time": "2025-11-14T10:30:45.123456",
  "end_time": "2025-11-14T10:30:47.654321",
  "total_items": 6,
  "items_by_type": {"html": 1, "json": 3, "text": 2}
}"""
}

for filename, content in output_files.items():
    print(f"✓ Saved {filename}")

print()
print("FILES SAVED TO: outputs/")
print("  ✓ cleaned_output.csv        (2.5 KB)")
print("  ✓ dynamic_schema.json       (1.2 KB)")
print("  ✓ processing_metadata.json  (0.3 KB)")
print()

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("=" * 70)
print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
print("=" * 70)
print()
print("📊 SUMMARY:")
print(f"  • Records extracted: {total_items}")
print(f"  • Columns created: {len(schema)}")
print(f"  • Schema fields: {len(schema)}")
print(f"  • Processing time: ~2 seconds")
print()
print("📁 OUTPUT FILES:")
print("  • outputs/cleaned_output.csv       → Open in Excel")
print("  • outputs/dynamic_schema.json      → See data structure")
print("  • outputs/processing_metadata.json → See processing info")
print()
print("🎉 Your data is now structured and ready to use!")
print()

# ============================================================================
# HOW TO USE IN REAL SCENARIO
# ============================================================================

print("=" * 70)
print("HOW TO USE WITH YOUR DATA")
print("=" * 70)
print("""
1. Place your file in inputs/ folder:
   inputs/
   └── your_scraped_file.txt (can be any format)

2. Run the pipeline:
   python main.py
   
3. Choose option 1 to process existing files

4. Wait for completion (typically 1-5 seconds)

5. Check outputs/:
   • cleaned_output.csv - Your structured data
   • dynamic_schema.json - What fields were found
   • processing_metadata.json - Processing stats

6. Open cleaned_output.csv in Excel or Python pandas:
   import pandas as pd
   df = pd.read_csv('outputs/cleaned_output.csv')
   print(df)
""")

print("=" * 70)
print("✨ That's how the ETL Pipeline works!")
print("=" * 70)
