"""Schema generation and diff utilities."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple


def _hash_dict(data: Dict[str, Any]) -> str:
    return hashlib.sha1(str(sorted(data.items())).encode("utf-8")).hexdigest()


def flatten(document: Any, prefix: str = "$") -> Dict[str, Any]:
    """Flatten nested dictionaries and lists using dotted paths."""
    items: Dict[str, Any] = {}
    if isinstance(document, dict):
        for key, value in document.items():
            child_prefix = f"{prefix}.{key}" if prefix else key
            items.update(flatten(value, child_prefix))
    elif isinstance(document, list):
        for idx, value in enumerate(document):
            child_prefix = f"{prefix}[{idx}]"
            items.update(flatten(value, child_prefix))
    else:
        items[prefix] = document
    return items


def infer_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "string"


class SchemaGenerator:
    """Create dynamic schemas and compute diffs."""

    def __init__(self, compatible_dbs: Iterable[str] | None = None):
        self.compatible_dbs = list(compatible_dbs or ["mongodb", "postgresql"])

    def build(
        self,
        source_id: str,
        version: int,
        records: List[Dict[str, Any]],
        previous_schema: Dict[str, Any] | None,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        field_stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "count": 0,
                "nullable": False,
                "examples": set(),
                "types": set(),
            }
        )

        for record in records:
            flattened = flatten(record["data"], prefix=f"$.{record['collection']}")
            for path, value in flattened.items():
                stats = field_stats[path]
                stats["count"] += 1
                if value is None:
                    stats["nullable"] = True
                else:
                    stats["examples"].add(str(value)[:80])
                    stats["types"].add(infer_type(value))

        total_records = max(1, len(records))
        fields = []
        primary_candidates = []
        for path, stats in sorted(field_stats.items()):
            confidence = min(0.99, stats["count"] / total_records)
            field_entry = {
                "name": path.split(".")[-1],
                "path": path,
                "types": sorted(stats["types"]) or ["string"],
                "nullable": stats["nullable"],
                "example_value": next(iter(stats["examples"]), None),
                "confidence": round(confidence, 2),
                "suggested_index": path.endswith("id"),
            }
            fields.append(field_entry)
            if field_entry["suggested_index"]:
                primary_candidates.append(field_entry["path"])

        schema_id = f"{source_id}_v{version}"
        schema = {
            "schema_id": schema_id,
            "source_id": source_id,
            "version": version,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "compatible_dbs": self.compatible_dbs,
            "fields": fields,
            "primary_key_candidates": primary_candidates,
        }

        schema["ddl"] = self._build_postgres_ddl(schema)
        diff = self._diff(previous_schema, schema)
        if diff:
            schema["migration_notes"] = diff.get("notes", "")
        return schema, diff

    def _build_postgres_ddl(self, schema: Dict[str, Any]) -> str:
        columns = []
        for field in schema["fields"]:
            pg_type = self._map_type(field["types"])
            column_name = self._normalize_column(field["path"])
            nullable = "" if field["nullable"] else " NOT NULL"
            columns.append(f"    {column_name} {pg_type}{nullable}")
        columns_str = ",\n".join(columns) or "    payload JSONB"
        return f"CREATE TABLE IF NOT EXISTS {schema['source_id']}_records (\n{columns_str}\n);"

    def _map_type(self, types: List[str]) -> str:
        if "integer" in types:
            return "BIGINT"
        if "number" in types:
            return "DOUBLE PRECISION"
        if "boolean" in types:
            return "BOOLEAN"
        if "array" in types:
            return "JSONB"
        if "object" in types:
            return "JSONB"
        return "TEXT"

    def _normalize_column(self, path: str) -> str:
        return (
            path.replace("$.", "")
            .replace("[", "_")
            .replace("]", "")
            .replace(".", "_")
            .lower()
        )

    def _diff(self, previous: Dict[str, Any] | None, current: Dict[str, Any]) -> Dict[str, Any]:
        if not previous:
            return {"added": [field["path"] for field in current["fields"]], "notes": "Initial schema."}

        prev_fields = {field["path"]: field for field in previous.get("fields", [])}
        curr_fields = {field["path"]: field for field in current.get("fields", [])}

        added = [path for path in curr_fields if path not in prev_fields]
        removed = [path for path in prev_fields if path not in curr_fields]
        changed = []

        for path, field in curr_fields.items():
            if path in prev_fields:
                prev_field = prev_fields[path]
                if set(field["types"]) != set(prev_field.get("types", [])) or field["nullable"] != prev_field.get("nullable"):
                    changed.append(path)

        notes_parts = []
        if added:
            notes_parts.append(f"Added fields: {', '.join(added)}")
        if removed:
            notes_parts.append(f"Removed fields: {', '.join(removed)}")
        if changed:
            notes_parts.append(f"Type/nullable changes: {', '.join(changed)}")

        return {"added": added, "removed": removed, "changed": changed, "notes": "; ".join(notes_parts)}


__all__ = ["SchemaGenerator", "flatten"]
