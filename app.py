#!/usr/bin/env python3
import sys
# Configure stdout/stderr for UTF-8 to prevent errors, especially on Windows
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except AttributeError:
    # This might fail in some environments (e.g., non-console)
    pass

from flask import Flask, request, jsonify
from flask_cors import CORS
import os, tempfile
from pathlib import Path
import pandas as pd
import traceback

app = Flask(__name__)
CORS(app)
Path('inputs').mkdir(exist_ok=True)
Path('outputs').mkdir(exist_ok=True)

@app.route('/', methods=['GET'])
def index():
    """Serves the main index.html frontend"""
    try:
        with open('index.html', encoding='utf-8') as f:
            return f.read(), 200, {'Content-Type': 'text/html; charset=utf-8'}
    except FileNotFoundError:
        print("ERROR: index.html not found!", flush=True)
        return "Error: index.html not found. Please ensure it's in the same directory.", 404

@app.route('/diagnostic.html', methods=['GET'])
def diagnostic():
    """Serves the diagnostic.html test page"""
    try:
        with open('diagnostic.html', encoding='utf-8') as f:
            return f.read(), 200, {'Content-Type': 'text/html; charset=utf-8'}
    except FileNotFoundError:
        return "diagnostic.html not found", 404

@app.route('/console_test.html', methods=['GET'])
def console_test():
    """Serves the console_test.html test page"""
    try:
        with open('console_test.html', encoding='utf-8') as f:
            return f.read(), 200, {'Content-Type': 'text/html; charset=utf-8'}
    except FileNotFoundError:
        return "console_test.html not found", 404

@app.route('/process', methods=['POST'])
def process():
    """Main API endpoint for processing data"""
    try:
        # Import the pipeline *inside* the endpoint.
        # This ensures any changes to etl_pipeline.py are picked up if reloader is used,
        # and it contains the import to the part of the code that needs it.
        from etl_pipeline import ETLPipeline
        
        content = request.get_data(as_text=True)
        if not content.strip():
            return jsonify({'error': 'No data provided'}), 400
        
        # Write content to a temporary file with UTF-8 encoding
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', dir='inputs', delete=False, encoding='utf-8') as f:
            f.write(content)
            fname = os.path.basename(f.name)
        
        print(f"DEBUG: Processing temporary file {fname}", flush=True)
        
        pipeline = ETLPipeline(input_dir='inputs', output_dir='outputs')
        df, schema = pipeline.run(fname)
        
        print(f"DEBUG: Pipeline run complete. {len(df)} records.", flush=True)
        
        # --- Type Inference and Data Serialization Logic ---
        # This logic is crucial for the frontend to display types correctly
        
        column_types = {}
        for col in df.columns:
            non_null_values = df[col].dropna()
            
            if len(non_null_values) == 0:
                column_types[col] = 'string' # Default for all-NaN columns
            elif non_null_values.apply(lambda x: isinstance(x, (list, tuple))).any():
                column_types[col] = 'array'
            elif pd.api.types.is_bool_dtype(non_null_values) or all(isinstance(v, bool) for v in non_null_values):
                column_types[col] = 'boolean'
            elif pd.api.types.is_numeric_dtype(non_null_values):
                column_types[col] = 'number'
            elif pd.api.types.is_datetime64_any_dtype(non_null_values):
                column_types[col] = 'datetime'
            else:
                column_types[col] = 'string'
        
        print(f"DEBUG - Column types detected: {column_types}", flush=True)
        
        # Convert DataFrame to a list of records (dicts) for JSON
        # This handles NaN/NaT values correctly for JSON serialization
        data = []
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                val = row[col]
                
                # Handle lists/arrays first
                if isinstance(val, (list, tuple)):
                    record[col] = val
                # Check for NaN/NaT (which are not JSON-safe)
                elif pd.isna(val):
                    record[col] = None # Convert to null
                # Handle boolean explicitly
                elif isinstance(val, bool):
                    record[col] = val
                # Handle numeric types (int, float)
                elif isinstance(val, (int, float)):
                    record[col] = val
                # Convert all other types to string
                else:
                    record[col] = str(val)
            data.append(record)
        
        # Clean up the temporary file
        try:
            os.remove(os.path.join('inputs', fname))
            print(f"DEBUG: Cleaned up temporary file {fname}", flush=True)
        except Exception as e:
            print(f"WARNING: Could not remove temp file {fname}: {e}", flush=True)
        
        # Return the complete response
        return jsonify({
            'success': True, 
            'data': data,         # The processed data rows
            'types': column_types # The inferred schema types
        }), 200
        
    except Exception as e:
        print(f"ERROR in process(): {str(e)}", flush=True)
        print(traceback.format_exc(), flush=True)
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

if __name__ == '__main__':
    print("="*60, flush=True)
    
    # --- THIS IS THE FIX ---
    # The original line "\U0001f680 ETL Pipeline Web Server" caused a UnicodeEncodeError
    print("ETL Pipeline Web Server", flush=True)
    # --- END OF FIX ---
    
    print("="*60, flush=True)
    print(f"Serving frontend from: {os.path.abspath('index.html')}", flush=True)
    print("Access at: http://127.0.0.1:5000", flush=True)
    print("Press CTRL+C to quit", flush=True)
    print("="*60, flush=True)
    
    # Run the Flask app
    app.run(host='127.0.0.1', port=5000, debug=False)