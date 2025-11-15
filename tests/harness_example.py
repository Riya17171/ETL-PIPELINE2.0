"""Reference test harness for the Dynamic ETL API."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import requests

API_ROOT = "http://127.0.0.1:8000"
SAMPLE_FILE = Path(__file__).resolve().parents[1] / "sample_data.txt"


def upload_sample(source_id: str) -> Dict:
    with SAMPLE_FILE.open("rb") as fh:
        resp = requests.post(
            f"{API_ROOT}/upload",
            files={"file": (SAMPLE_FILE.name, fh, "text/plain")},
            data={"source_id": source_id},
            timeout=60,
        )
    resp.raise_for_status()
    return resp.json()


def fetch_schema(source_id: str) -> Dict:
    resp = requests.get(f"{API_ROOT}/schema", params={"source_id": source_id}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ask_question(source_id: str, question: str) -> Dict:
    resp = requests.post(
        f"{API_ROOT}/query",
        json={"source_id": source_id, "nl_query": question},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    source_id = "harness-demo"
    print("Uploading sample file...")
    upload_info = upload_sample(source_id)
    print(json.dumps(upload_info, indent=2))

    print("\nFetching latest schema...")
    schema = fetch_schema(source_id)
    print(json.dumps(schema, indent=2))

    print("\nRunning natural language query...")
    query = ask_question(source_id, "count all records")
    print(json.dumps(query, indent=2))


if __name__ == "__main__":
    main()
