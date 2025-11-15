#!/usr/bin/env python3
import sys
print("Python started", flush=True)
sys.stdout.flush()

from flask import Flask
print("Flask imported", flush=True)

app = Flask(__name__)

@app.route('/')
def hello():
    return 'OK'

if __name__ == '__main__':
    print("Starting test Flask app...", flush=True)
    app.run(host='127.0.0.1', port=5001, debug=False)
