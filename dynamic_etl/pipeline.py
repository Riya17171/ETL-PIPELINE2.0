"""Dynamic ETL pipeline orchestrator."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from .ingestion import FileIngestor
from .parsers import ParserEngine
from .schema import SchemaGenerator
from .storage import MongoStorage


@dataclass
class PipelineResult:
    source_id: str
    version: int
    schema: Dict[str, any]
    diff: Dict[str, any]
    records: List[Dict[str, any]]
    summary: Dict[str, any]
    fragments: List[Dict[str, any]]
    file_id: str


class DynamicETLPipeline:
    """Complete pipeline from file ingest to storage and schema generation."""

    def __init__(self, input_dir: str = "inputs", output_dir: str = "outputs", storage: MongoStorage | None = None):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.input_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.ingestor = FileIngestor(self.input_dir)
        self.parser = ParserEngine()
        self.storage = storage or MongoStorage()
        self.schema_generator = SchemaGenerator()

    def process(self, filename: str, source_id: str, binary: bytes | None = None, version: int | None = None) -> PipelineResult:
        text, payload = self.ingestor.read(filename, binary=binary)
        fragments, summary, records = self.parser.parse(text)

        if version is None:
            version = self.storage.latest_version(source_id) + 1

        previous_schema = self.storage.get_schema(source_id, version - 1)
        schema, diff = self.schema_generator.build(source_id, version, records, previous_schema)

        self.storage.save_records(source_id, version, records)
        self.storage.save_schema(schema, diff)

        file_id = f"{source_id}_{uuid.uuid4().hex[:8]}"
        return PipelineResult(
            source_id=source_id,
            version=version,
            schema=schema,
            diff=diff,
            records=records,
            summary=summary,
            fragments=[fragment.__dict__ for fragment in fragments],
            file_id=file_id,
        )


__all__ = ["DynamicETLPipeline", "PipelineResult"]
