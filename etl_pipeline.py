import os
import json
import re
import base64
import pandas as pd
import sqlite3
from pathlib import Path
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Tuple
import mimetypes
from datetime import datetime
import sys
import io

# Fix encoding issues on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

class ETLPipeline:
    def __init__(self, input_dir="inputs", output_dir="outputs", use_db=False):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.extracted_data = []
        self.schema = {}
        self.use_db = use_db
        self.db_path = self.output_dir / "etl_data.db" if use_db else None
        self.processing_metadata = {
            'start_time': None,
            'end_time': None,
            'filename': None,
            'total_items': 0,
            'items_by_type': {}
        }
        
        if use_db:
            self._init_database()
        
    def read_file(self, filename: str) -> str:
        """Step 1: Read the entire mixed file"""
        filepath = self.input_dir / filename
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return f.read()
        except UnicodeDecodeError:
            try:
                with open(filepath, 'r', encoding='latin-1') as f:
                    return f.read()
            except Exception as e:
                raise ValueError(f"Cannot read file with available encodings: {str(e)}")
    
    def detect_content_types(self, content: str) -> Dict[str, List[str]]:
        """Detect different content types in the mixed file"""
        detected = {
            'html': [],
            'json': [],
            'text': [],
            'base64': []
        }
        
        # Detect HTML blocks - more comprehensive patterns
        html_patterns = [
            r'<html[^>]*>.*?</html>',
            r'<!DOCTYPE[^>]*>.*?</html>',
            r'<div[^>]*>.*?</div>',
            r'<p[^>]*>.*?</p>',
            r'<body[^>]*>.*?</body>'
        ]
        for pattern in html_patterns:
            html_matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)
            detected['html'].extend(html_matches)
        
        # Remove duplicates
        detected['html'] = list(set(detected['html']))
        
        # Detect JSON blocks - improved pattern
        json_pattern = r'\{(?:[^{}]|(?:\{[^{}]*\}))*\}'
        json_matches = re.findall(json_pattern, content, re.DOTALL)
        for match in json_matches:
            try:
                json.loads(match)
                if match not in detected['json']:
                    detected['json'].append(match)
            except:
                pass
        
        # Detect base64 encoded data
        base64_patterns = [
            r'data:image/[^;]+;base64,([A-Za-z0-9+/=]+)',
            r'data:text/[^;]+;base64,([A-Za-z0-9+/=]+)',
            r'([A-Za-z0-9+/]{64,}={0,2})'  # Generic base64 strings
        ]
        for pattern in base64_patterns:
            base64_matches = re.findall(pattern, content)
            detected['base64'].extend(base64_matches)
        
        detected['base64'] = list(set(detected['base64']))
        
        # Extract plain text (everything else)
        remaining_text = content
        for html in detected['html']:
            remaining_text = remaining_text.replace(html, '')
        for json_str in detected['json']:
            remaining_text = remaining_text.replace(json_str, '')
        
        # Split into paragraphs
        paragraphs = [p.strip() for p in remaining_text.split('\n') if p.strip() and len(p.strip()) > 5]
        detected['text'] = paragraphs
        
        return detected
    
    def extract_html(self, html_string: str) -> Dict[str, Any]:
        """Extract data from HTML - only keep: type, title, word_count"""
        soup = BeautifulSoup(html_string, 'html.parser')
        
        return {
            'type': 'html',
            'title': soup.title.string if soup.title else '',
            'word_count': len(soup.get_text().split())
        }
    
    def extract_json(self, json_string: str) -> Dict[str, Any]:
        """Extract and flatten JSON data - NO word_count/title (JSON has its own fields)"""
        try:
            data = json.loads(json_string)
            flattened = self.flatten_dict(data)
            # IMPORTANT: Only add type, don't add word_count/title
            # JSON objects have their own natural fields
            flattened['type'] = 'json'
            return flattened
        except:
            return {'type': 'json', 'error': 'Invalid JSON', 'raw': json_string[:100]}
    
    def flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Flatten nested dictionary - preserves arrays and primitives"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Keep arrays as lists, don't convert to JSON string
                items.append((new_key, v))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def extract_text(self, text: str) -> Dict[str, Any]:
        """Extract data from plain text - only keep: type, title, word_count"""
        return {
            'type': 'text',
            'title': text[:50] if len(text) > 50 else text,
            'word_count': len(text.split())
        }
    
    def extract_media(self, base64_data: str) -> Dict[str, Any]:
        """Extract media metadata - only keep: type, title, word_count"""
        return {
            'type': 'media',
            'title': 'Base64 Media',
            'word_count': 0
        }
    
    def extract(self, content: str):
        """Step 2: Extract data from all content types"""
        detected = self.detect_content_types(content)
        
        # Process HTML
        for idx, html in enumerate(detected['html']):
            extracted = self.extract_html(html)
            extracted['source_index'] = f"html_{idx}"
            self.extracted_data.append(extracted)
        
        # Process JSON
        for idx, json_str in enumerate(detected['json']):
            extracted = self.extract_json(json_str)
            extracted['source_index'] = f"json_{idx}"
            self.extracted_data.append(extracted)
        
        # Process Text
        for idx, text in enumerate(detected['text']):
            extracted = self.extract_text(text)
            extracted['source_index'] = f"text_{idx}"
            self.extracted_data.append(extracted)
        
        # Process Media
        for idx, media in enumerate(detected['base64']):
            extracted = self.extract_media(media)
            extracted['source_index'] = f"media_{idx}"
            self.extracted_data.append(extracted)
    
    def infer_schema(self):
        """Step 3: Build dynamic schema from extracted data"""
        all_keys = set()
        for item in self.extracted_data:
            all_keys.update(item.keys())
        
        # Build schema with type inference
        for key in all_keys:
            values = [item.get(key) for item in self.extracted_data if key in item]
            value_types = set(type(v).__name__ for v in values if v is not None)
            
            self.schema[key] = {
                'type': list(value_types) if value_types else ['NoneType'],
                'nullable': any(v is None for v in [item.get(key) for item in self.extracted_data]),
                'present_in': sum(1 for item in self.extracted_data if key in item)
            }
        
        # Fill missing values with None
        for item in self.extracted_data:
            for key in all_keys:
                if key not in item:
                    item[key] = None
    
    def normalize(self) -> pd.DataFrame:
        """Step 4: Normalize data into DataFrame with total_items
        
        Key Strategy: Keep record types separate in their own row groups to avoid
        polluting HTML/Text records with JSON fields when mixed content is processed.
        """
        # Remove word_count and title from all records - they're extraction artifacts
        cleaned_data = []
        for record in self.extracted_data:
            cleaned = {k: v for k, v in record.items() if k not in ['word_count', 'title']}
            cleaned_data.append(cleaned)
        
        # Group by type to keep fields separate
        grouped_by_type = {}
        for record in cleaned_data:
            record_type = record.get('type', 'unknown')
            if record_type not in grouped_by_type:
                grouped_by_type[record_type] = []
            grouped_by_type[record_type].append(record)
        
        # Build DataFrame with type-specific columns
        all_rows = []
        for record_type in ['html', 'json', 'text', 'media']:  # Process in order
            if record_type not in grouped_by_type:
                continue
            
            type_records = grouped_by_type[record_type]
            type_df = pd.DataFrame(type_records)
            
            # Only include type-specific fields + core fields
            # For each type, keep only the columns that actually have data
            core_fields = ['type', 'source_index']
            type_specific = [col for col in type_df.columns if col not in core_fields]
            
            # Reorder columns: core first, then type-specific
            cols_to_keep = core_fields + type_specific
            type_df = type_df[cols_to_keep]
            
            all_rows.append(type_df)
        
        # Combine all type groups
        if all_rows:
            df = pd.concat(all_rows, ignore_index=True, sort=False)
        else:
            df = pd.DataFrame()
        
        if len(df) == 0:
            return df
        
        # Add total_items column to every row
        total_items = len(df)
        df['total_items'] = total_items
        
        # Reorder: type, source_index, total_items, then everything else
        core_order = ['type', 'source_index', 'total_items']
        other_cols = [col for col in df.columns if col not in core_order]
        final_cols = core_order + sorted(other_cols)
        df = df[final_cols]
        
        # Ensure numeric columns stay numeric
        for col in ['total_items']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        return df
    
    def load(self, df: pd.DataFrame, output_csv="cleaned_output.csv", 
             schema_json="dynamic_schema.json"):
        """Step 5: Save outputs"""
        # Save CSV
        csv_path = self.output_dir / output_csv
        df.to_csv(csv_path, index=False)
        print(f"Saved cleaned data to: {csv_path}")
        
        # Save schema
        schema_path = self.output_dir / schema_json
        with open(schema_path, 'w') as f:
            json.dump(self.schema, f, indent=2)
        print(f"Saved schema to: {schema_path}")
        
        # Save metadata
        metadata_path = self.output_dir / "processing_metadata.json"
        self.processing_metadata['end_time'] = datetime.now().isoformat()
        self.processing_metadata['total_items'] = len(df)
        with open(metadata_path, 'w') as f:
            json.dump(self.processing_metadata, f, indent=2)
        print(f"Saved metadata to: {metadata_path}")
        
        # Save to SQLite if enabled
        if self.use_db:
            self._save_to_db(df)
        
        # Print summary
        print(f"\nETL Summary:")
        print(f"   - Records extracted: {len(df)}")
        print(f"   - Columns: {len(df.columns)}")
        print(f"   - Schema fields: {len(self.schema)}")
        
        return csv_path, schema_path
    
    def _init_database(self):
        """Initialize SQLite database"""
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    source_index TEXT,
                    data_type TEXT,
                    data_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS schemas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    schema_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            print(f"SQLite database initialized: {self.db_path}")
        except Exception as e:
            print(f"Warning: Could not initialize database: {str(e)}")
    
    def _save_to_db(self, df: pd.DataFrame):
        """Save processed data to SQLite database"""
        try:
            conn = sqlite3.connect(str(self.db_path))
            cursor = conn.cursor()
            
            # Insert data
            for _, row in df.iterrows():
                data_json = json.dumps(row.to_dict())
                data_type = row.get('type', 'unknown')
                source_index = row.get('source_index', '')
                
                cursor.execute('''
                    INSERT INTO processed_data (filename, source_index, data_type, data_json)
                    VALUES (?, ?, ?, ?)
                ''', (self.processing_metadata['filename'], source_index, data_type, data_json))
            
            # Insert schema
            schema_json = json.dumps(self.schema)
            cursor.execute('''
                INSERT INTO schemas (filename, schema_json)
                VALUES (?, ?)
            ''', (self.processing_metadata['filename'], schema_json))
            
            conn.commit()
            conn.close()
            print(f"Data saved to SQLite database")
        except Exception as e:
            print(f"Warning: Could not save to database: {str(e)}")
    
    def run(self, filename: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Run the complete ETL pipeline"""
        self.processing_metadata['start_time'] = datetime.now().isoformat()
        self.processing_metadata['filename'] = filename
        
        print(f"Starting ETL Pipeline for: {filename}")
        print("=" * 50)
        
        try:
            # Step 1: Read
            print("\n[1] Reading file...")
            content = self.read_file(filename)
            print(f"   File size: {len(content)} characters")
            
            # Step 2: Extract
            print("\n[2] Extracting data...")
            self.extract(content)
            print(f"   Extracted {len(self.extracted_data)} items")
            
            # Track items by type
            for item in self.extracted_data:
                item_type = item.get('type', 'unknown')
                self.processing_metadata['items_by_type'][item_type] = \
                    self.processing_metadata['items_by_type'].get(item_type, 0) + 1
            
            # Step 3: Infer Schema
            print("\n[3] Inferring schema...")
            self.infer_schema()
            print(f"   Schema with {len(self.schema)} fields")
            
            # Step 4: Normalize
            print("\n[4] Normalizing data...")
            df = self.normalize()
            print(f"   Created DataFrame: {df.shape}")
            
            # Step 5: Load
            print("\n[5] Loading outputs...")
            csv_path, schema_path = self.load(df)
            
            print("\nETL Pipeline completed successfully!")
            print("=" * 50)
            
            return df, self.schema
            
        except Exception as e:
            print(f"\nError during ETL pipeline: {str(e)}")
            self.processing_metadata['end_time'] = datetime.now().isoformat()
            self.processing_metadata['error'] = str(e)
            raise


if __name__ == "__main__":
    # Example usage
    pipeline = ETLPipeline()
    
    # Run pipeline on a file
    # pipeline.run("raw_scraped_file.txt")
    
    print("ETL Pipeline ready! Usage:")
    print("pipeline = ETLPipeline()")
    print('pipeline.run("your_file.txt")')