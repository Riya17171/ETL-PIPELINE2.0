from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
import traceback
import tempfile
import os
import json
from pathlib import Path
import pandas as pd

app = Flask(__name__)
CORS(app)

# Configure JSON encoder to handle special values
class SafeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            if pd.isna(obj):
                return None
        except:
            pass
        return super().default(obj)

app.json_encoder = SafeJSONEncoder

UPLOAD_FOLDER = 'inputs'
OUTPUT_FOLDER = 'outputs'
Path(UPLOAD_FOLDER).mkdir(exist_ok=True)
Path(OUTPUT_FOLDER).mkdir(exist_ok=True)

print("Flask app initialized", flush=True)

@app.route('/', methods=['GET'])
def index():
    """Serve the HTML interface"""
    try:
        with open('index.html', 'r', encoding='utf-8') as f:
            html = f.read()
        return html, 200, {'Content-Type': 'text/html'}
    except Exception as e:
        print(f"Error serving index: {e}", flush=True)
        traceback.print_exc()
        return f"Error: {str(e)}", 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})

@app.route('/process', methods=['POST'])
def process():
    """Process data"""
    try:
        print("Process request received", flush=True)
        content = request.get_data(as_text=True)
        
        if not content or not content.strip():
            return jsonify({'error': 'No data provided'}), 400
        
        # Clean content - remove problematic characters
        try:
            content = content.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
        except:
            pass
        
        print(f"Processing {len(content)} characters", flush=True)
        
        # Try to import ETL pipeline
        try:
            from etl_pipeline import ETLPipeline
            print("ETL Pipeline imported", flush=True)
            
            # Create temp file with UTF-8 encoding
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', dir=UPLOAD_FOLDER, delete=False, encoding='utf-8') as f:
                f.write(content)
                temp_filename = os.path.basename(f.name)
            
            print(f"Processing file: {temp_filename}", flush=True)
            
            # Run pipeline
            pipeline = ETLPipeline(input_dir=UPLOAD_FOLDER, output_dir=OUTPUT_FOLDER)
            df, schema = pipeline.run(temp_filename)
            
            print(f"Pipeline completed: {len(df)} records", flush=True)
            
            # Convert to dict with error handling
            try:
                # Replace NaN and None with empty strings
                df = df.fillna('')
                # Convert all columns to string to ensure JSON serializable
                for col in df.columns:
                    df[col] = df[col].astype(str)
                data = df.to_dict('records')
            except Exception as convert_error:
                print(f"Conversion error: {convert_error}", flush=True)
                traceback.print_exc()
                # If conversion still fails, convert everything to string first
                try:
                    df = df.astype(str).fillna('')
                    data = df.to_dict('records')
                except:
                    data = []
            
            # Cleanup
            try:
                os.remove(os.path.join(UPLOAD_FOLDER, temp_filename))
            except:
                pass
            
            return jsonify({
                'success': True,
                'data': data,
                'summary': {
                    'records': len(df),
                    'columns': len(df.columns)
                }
            }), 200
            
        except ImportError as e:
            print(f"Import error: {e}", flush=True)
            return jsonify({'error': f'Import error: {str(e)}'}), 500
        except Exception as pipeline_error:
            print(f"Pipeline error: {pipeline_error}", flush=True)
            traceback.print_exc()
            return jsonify({'error': f'Processing error: {str(pipeline_error)}'}), 500
            
    except Exception as e:
        print(f"Request error: {e}", flush=True)
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    print(f"404 error: {request.path}", flush=True)
    return jsonify({'error': 'not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    print(f"500 error: {error}", flush=True)
    traceback.print_exc()
    return jsonify({'error': 'internal server error'}), 500

if __name__ == '__main__':
    print("\n" + "="*60, flush=True)
    print("ETL Pipeline Web Server", flush=True)
    print("="*60, flush=True)
    print("Access at: http://localhost:5000", flush=True)
    print("="*60 + "\n", flush=True)
    
    try:
        print("Starting Flask server...", flush=True)
        app.run(
            debug=False, 
            port=5000, 
            use_reloader=False, 
            host='127.0.0.1',
            threaded=True
        )
    except Exception as e:
        print(f"FATAL ERROR: {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)
