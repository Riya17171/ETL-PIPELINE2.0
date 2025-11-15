"""
Simple ETL Pipeline - Main Entry Point
No fancy APIs, just straightforward file processing
"""

import os
import sys
from pathlib import Path
from etl_pipeline import ETLPipeline
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class InputFileHandler(FileSystemEventHandler):
    """Watch for new files in the input folder"""
    
    def __init__(self, output_callback):
        self.output_callback = output_callback
    
    def on_created(self, event):
        if event.is_dir:
            return
        
        # Wait a moment for the file to be fully written
        time.sleep(1)
        
        filename = Path(event.src_path).name
        print(f"\nNew file detected: {filename}")
        
        # Process the file
        try:
            self.output_callback(filename)
        except Exception as e:
            print(f"Error processing file: {str(e)}")


class SimpleETL:
    """Simple ETL handler without APIs"""
    
    def __init__(self, input_dir="inputs", output_dir="outputs", use_db=False, watch_mode=False):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.use_db = use_db
        self.watch_mode = watch_mode
        self.pipeline = ETLPipeline(input_dir=input_dir, output_dir=output_dir, use_db=use_db)
    
    def process_file(self, filename):
        """Process a single file through the ETL pipeline"""
        print("\n" + "="*60)
        print(f"Processing: {filename}")
        print("="*60)
        
        try:
            df, schema = self.pipeline.run(filename)
            print(f"\nSuccess! Results saved to /{self.output_dir}/")
            print(f"   - cleaned_output.csv")
            print(f"   - dynamic_schema.json")
            print(f"   - processing_metadata.json")
            if self.use_db:
                print(f"   - etl_data.db")
            return True
        except Exception as e:
            print(f"\nFailed: {str(e)}")
            return False
    
    def start_watch_mode(self):
        """Start watching the input folder for new files"""
        print(f"\nWatch Mode: Monitoring {self.input_dir}/ for new files...")
        print("(Press Ctrl+C to stop)\n")
        
        observer = Observer()
        handler = InputFileHandler(self.process_file)
        observer.schedule(handler, self.input_dir, recursive=False)
        observer.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping file watcher...")
            observer.stop()
        observer.join()
    
    def process_existing_files(self):
        """Process all files currently in the input folder"""
        input_path = Path(self.input_dir)
        files = list(input_path.glob('*'))
        
        # Filter out directories
        files = [f for f in files if f.is_file()]
        
        if not files:
            print(f"No files found in {self.input_dir}/")
            return
        
        print(f"\nFound {len(files)} file(s) to process")
        
        for file_path in files:
            filename = file_path.name
            self.process_file(filename)
            print("\n" + "-"*60 + "\n")


def setup_directories():
    """Create necessary directories"""
    Path("inputs").mkdir(exist_ok=True)
    Path("outputs").mkdir(exist_ok=True)
    print("Directories ready: inputs/ and outputs/")


def show_menu():
    """Display the main menu"""
    print("\n" + "="*60)
    print("SIMPLE ETL PIPELINE")
    print("="*60)
    print("\nOptions:")
    print("1. Process existing files in inputs/ folder")
    print("2. Watch inputs/ folder for new files (auto-process)")
    print("3. Process a specific file")
    print("4. View outputs")
    print("5. Exit")
    print("-"*60)


def view_outputs():
    """Display information about output files"""
    output_path = Path("outputs")
    
    if not output_path.exists():
        print("\nNo outputs/ folder found")
        return
    
    files = list(output_path.glob('*'))
    
    if not files:
        print("\nNo output files found. Process a file first!")
        return
    
    print("\nOutput Files:")
    for file in files:
        size = file.stat().st_size
        size_mb = size / (1024 * 1024)
        print(f"   {file.name} ({size_mb:.2f} MB)")
    
    # Show latest metadata
    metadata_file = output_path / "processing_metadata.json"
    if metadata_file.exists():
        import json
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        print(f"\nLatest Processing Info:")
        print(f"   - File: {metadata.get('filename')}")
        print(f"   - Items by type: {metadata.get('items_by_type')}")
        print(f"   - Total items: {metadata.get('total_items')}")


def main():
    """Main entry point"""
    setup_directories()
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "watch":
            # Watch mode
            etl = SimpleETL(use_db=False, watch_mode=True)
            etl.start_watch_mode()
        elif command == "process":
            # Process existing files
            etl = SimpleETL(use_db=False)
            etl.process_existing_files()
        elif command == "db":
            # Process with database
            etl = SimpleETL(use_db=True)
            etl.process_existing_files()
        else:
            print(f"Unknown command: {command}")
            print("\nUsage:")
            print("  python main.py              (Interactive menu)")
            print("  python main.py watch        (Watch and auto-process files)")
            print("  python main.py process      (Process existing files)")
            print("  python main.py db           (Process with SQLite database)")
    else:
        # Interactive menu
        while True:
            show_menu()
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == "1":
                etl = SimpleETL(use_db=False)
                etl.process_existing_files()
            
            elif choice == "2":
                etl = SimpleETL(use_db=False, watch_mode=True)
                etl.start_watch_mode()
            
            elif choice == "3":
                filename = input("\nEnter filename to process: ").strip()
                if filename:
                    etl = SimpleETL(use_db=False)
                    etl.process_file(filename)
                else:
                    print("No filename provided")
            
            elif choice == "4":
                view_outputs()
            
            elif choice == "5":
                print("\nGoodbye!")
                break
            
            else:
                print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
