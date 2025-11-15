"""Local LLM assisted query translator."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class TranslationResult:
    nl_query: str
    translated_query: Dict[str, Any]
    raw_response: str


class LLMQueryEngine:
    """Translate natural language questions into MongoDB queries."""

    def __init__(self, model_path: str | None = None):
        self.model = None
        self.model_path = model_path or os.environ.get("LLM_MODEL_PATH")
        self._load_model()

    def _load_model(self) -> None:
        if not self.model_path:
            return
        try:
            from pathlib import Path

            from gpt4all import GPT4All

            model_path = Path(self.model_path)
            if model_path.is_file():
                self.model = GPT4All(model_name=model_path.name, model_path=str(model_path.parent))
            else:
                self.model = GPT4All(model_name=self.model_path)
        except Exception:
            self.model = None

    def translate(self, nl_query: str, schema: Dict[str, Any]) -> TranslationResult:
        """Return a MongoDB find/aggregate specification derived from NL query."""
        if self.model is not None:
            prompt = self._build_prompt(nl_query, schema)
            response = self.model.generate(prompt, max_tokens=256)
            parsed = self._parse_response(response)
            if parsed:
                return TranslationResult(nl_query, parsed, response)
        # Fallback deterministic translation
        parsed = self._heuristic_translation(nl_query, schema)
        return TranslationResult(nl_query, parsed, json.dumps(parsed))

    def _build_prompt(self, nl_query: str, schema: Dict[str, Any]) -> str:
        field_lines = [f"- {field['path']} types={field['types']}" for field in schema.get("fields", [])]
        schema_description = "\n".join(field_lines)
        return (
            "You translate natural language to MongoDB queries. "
            "Respond with JSON containing either a 'find' object or an 'aggregate' pipeline list.\n"
            f"Schema fields:\n{schema_description}\n"
            f"Question: {nl_query}\n"
            "Response JSON:"
        )

    def _parse_response(self, response: str) -> Optional[Dict[str, Any]]:
        try:
            start = response.index("{")
            json_text = response[start:]
            return json.loads(json_text)
        except Exception:
            return None

    def _heuristic_translation(self, nl_query: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        query = {"find": {}, "projection": {"_id": 0}}
        lower = nl_query.lower()
        # Detect equality conditions like "where title is Widget A"
        if "where" in lower and " is " in lower:
            clause = lower.split("where", 1)[1].strip()
            if " is " in clause:
                field, value = clause.split(" is ", 1)
                query["find"][field.strip().replace(" ", "_")] = value.strip().strip("\'.")
        if "count" in lower:
            field = None
            for candidate in schema.get("primary_key_candidates", []):
                field = candidate
                break
            pipeline = []
            if query["find"]:
                pipeline.append({"$match": query["find"]})
            pipeline.append({"$count": "total"})
            return {"aggregate": pipeline}
        if "average" in lower or "avg" in lower:
            numeric_field = self._pick_numeric_field(schema)
            pipeline = []
            if query["find"]:
                pipeline.append({"$match": query["find"]})
            pipeline.append({"$group": {"_id": None, "average": {"$avg": f"${numeric_field}"}}})
            return {"aggregate": pipeline}
        return query

    def _pick_numeric_field(self, schema: Dict[str, Any]) -> str:
        for field in schema.get("fields", []):
            if any(t in ("integer", "number") for t in field.get("types", [])):
                return field["path"].replace("$.", "")
        return schema.get("primary_key_candidates", ["value"])[0]


__all__ = ["LLMQueryEngine", "TranslationResult"]
