"""Quick preview runner for the dynamic ETL pipeline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable

from dynamic_etl import DynamicETLPipeline, MongoStorage


def _print_heading(title: str) -> None:
    print("\n" + title)
    print("=" * len(title))


def _print_json_block(title: str, payload: Dict[str, Any]) -> None:
    _print_heading(title)
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _print_records(records: Iterable[Dict[str, Any]], limit: int = 3) -> None:
    _print_heading(f"Sample records (showing up to {limit})")
    for index, record in enumerate(records):
        if index >= limit:
            print("…")
            break
        print(f"- collection: {record.get('collection', 'unknown')}")
        print(json.dumps(record.get("data", {}), indent=2, ensure_ascii=False))


def run_preview(file_path: Path, source_id: str, version: int | None) -> None:
    storage = MongoStorage()
    pipeline = DynamicETLPipeline(storage=storage)

    result = pipeline.process(
        filename=file_path.name,
        source_id=source_id,
        binary=file_path.read_bytes(),
        version=version,
    )

    _print_heading("Processing summary")
    for key, value in sorted(result.summary.items()):
        print(f"{key}: {value}")

    _print_json_block("Generated schema", result.schema)
    _print_records(result.records)

    schema_file = Path("outputs/schemas") / source_id / f"schema_v{result.version}.json"
    records_file = Path("outputs/records") / source_id / f"records_v{result.version}.json"

    _print_heading("Local artifacts")
    print(f"Schema saved to: {schema_file}")
    print(f"Records snapshot: {records_file}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview the dynamic ETL pipeline on a local file")
    parser.add_argument("file", type=Path, help="Path to the input file (txt/md/pdf)")
    parser.add_argument("--source-id", default="preview", help="Namespace for the upload (default: preview)")
    parser.add_argument("--version", type=int, default=None, help="Override the schema version")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    file_path: Path = args.file

    if not file_path.exists():
        raise SystemExit(f"Input file not found: {file_path}")

    run_preview(file_path=file_path, source_id=args.source_id, version=args.version)


if __name__ == "__main__":
    main()
