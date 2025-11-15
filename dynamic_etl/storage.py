"""MongoDB storage helpers with local fallbacks."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.errors import ServerSelectionTimeoutError

try:
    import mongomock
except ImportError:  # pragma: no cover - optional dependency
    mongomock = None


class MongoStorage:
    """Wrapper around MongoDB with a local JSON fallback."""

    def __init__(self, uri: str | None = None, db_name: str = "dynamic_etl"):
        uri = uri or os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
        self.client = self._create_client(uri)
        self.db = self.client[db_name]
        self._prepare_indexes()
        self.local_output_dir = Path("outputs")
        self.local_output_dir.mkdir(exist_ok=True)

    def _create_client(self, uri: str) -> MongoClient:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.server_info()
            return client
        except ServerSelectionTimeoutError:
            if mongomock is None:
                raise
            return mongomock.MongoClient()

    def _prepare_indexes(self) -> None:
        self.schemas.create_index([("source_id", ASCENDING), ("version", ASCENDING)], unique=True)
        self.records.create_index([("source_id", ASCENDING), ("version", ASCENDING)])
        self.queries.create_index([("source_id", ASCENDING), ("created_at", ASCENDING)])

    # Collections -------------------------------------------------------

    @property
    def schemas(self) -> Collection:
        return self.db["schemas"]

    @property
    def records(self) -> Collection:
        return self.db["records"]

    @property
    def queries(self) -> Collection:
        return self.db["queries"]

    # Data persistence --------------------------------------------------

    def latest_version(self, source_id: str) -> int:
        doc = self.schemas.find_one({"source_id": source_id}, sort=[("version", -1)])
        return int(doc["version"]) if doc else 0

    def save_schema(self, schema: Dict[str, Any], diff: Dict[str, Any]) -> None:
        payload = {
            **schema,
            "diff": diff,
            "stored_at": datetime.utcnow().isoformat() + "Z",
        }
        self.schemas.replace_one(
            {"source_id": schema["source_id"], "version": schema["version"]},
            payload,
            upsert=True,
        )
        self._persist_local_schema(schema)

    def save_records(self, source_id: str, version: int, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        docs = [
            {
                "source_id": source_id,
                "version": version,
                "collection": record["collection"],
                "data": record["data"],
                "source_range": record.get("source_range"),
                "created_at": datetime.utcnow().isoformat() + "Z",
            }
            for record in records
        ]
        self.records.insert_many(docs)
        self._persist_local_records(source_id, version, records)

    def _persist_local_schema(self, schema: Dict[str, Any]) -> None:
        source_dir = self.local_output_dir / "schemas" / schema["source_id"]
        source_dir.mkdir(parents=True, exist_ok=True)
        schema_path = source_dir / f"schema_v{schema['version']}.json"
        schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")

    def _persist_local_records(self, source_id: str, version: int, records: List[Dict[str, Any]]) -> None:
        source_dir = self.local_output_dir / "records" / source_id
        source_dir.mkdir(parents=True, exist_ok=True)
        records_path = source_dir / f"records_v{version}.json"
        records_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

    # Retrieval ---------------------------------------------------------

    def get_schema(self, source_id: str, version: Optional[int] = None) -> Optional[Dict[str, Any]]:
        query = {"source_id": source_id}
        sort = [("version", -1)]
        if version is not None:
            query["version"] = version
            sort = None
        return self.schemas.find_one(query, sort=sort)

    def get_schema_history(self, source_id: str) -> List[Dict[str, Any]]:
        cursor = self.schemas.find({"source_id": source_id}).sort("version", ASCENDING)
        return list(cursor)

    def get_records(self, source_id: str, version: Optional[int] = None, limit: int = 1000) -> List[Dict[str, Any]]:
        query = {"source_id": source_id}
        if version is not None:
            query["version"] = version
        cursor = self.records.find(query).limit(limit)
        return list(cursor)

    # Query persistence -------------------------------------------------

    def store_query_result(
        self,
        source_id: str,
        nl_query: str,
        translated_query: Dict[str, Any],
        results: List[Dict[str, Any]],
    ) -> str:
        doc = {
            "source_id": source_id,
            "nl_query": nl_query,
            "translated_query": translated_query,
            "results": results,
            "created_at": datetime.utcnow().isoformat() + "Z",
        }
        inserted = self.queries.insert_one(doc)
        return str(inserted.inserted_id)

    def get_query_result(self, query_id: str) -> Optional[Dict[str, Any]]:
        from bson import ObjectId

        try:
            obj_id = ObjectId(query_id)
        except Exception:
            return None
        return self.queries.find_one({"_id": obj_id})


__all__ = ["MongoStorage"]
