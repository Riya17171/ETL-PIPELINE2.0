"""Parsers for extracting structure from unstructured documents."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from io import StringIO
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
import regex as re
from bs4 import BeautifulSoup
import yaml

logger = logging.getLogger(__name__)


@dataclass
class ParsedFragment:
    """Metadata describing a discovered fragment inside a document."""

    fragment_type: str
    start: int
    end: int
    preview: str
    payload: Any
    extra: Dict[str, Any] = field(default_factory=dict)


class ParserEngine:
    """High level parser coordinating specialized extractors."""

    JSON_PATTERN = re.compile(
        r"\{(?:[^{}]|(?R))*\}",  # recursive pattern to capture nested braces
        re.MULTILINE,
    )

    KV_PATTERN = re.compile(r"^(?P<key>[\w\- ]{2,}):\s*(?P<value>.+)$", re.MULTILINE)

    CSV_SECTION_PATTERN = re.compile(
        r"(?P<header>(?:[^\n,]+,)+[^\n,]+)\n(?P<body>(?:[^\n]+\n?){1,})",
        re.MULTILINE,
    )

    YAML_PATTERN = re.compile(r"---\s*\n(?P<yaml>.*?)(?:\n---|$)", re.DOTALL)

    def parse(self, text: str) -> Tuple[List[ParsedFragment], Dict[str, Any], List[Dict[str, Any]]]:
        """Parse text and return fragments, summary and extracted records."""
        fragments: List[ParsedFragment] = []
        records: List[Dict[str, Any]] = []
        summary = {
            "json_fragments": 0,
            "html_tables": 0,
            "kv_pairs": 0,
            "csv_blocks": 0,
            "yaml_blocks": 0,
            "text_blocks": 0,
        }

        fragments.extend(self._parse_json_fragments(text, summary, records))
        fragments.extend(self._parse_html_fragments(text, summary, records))
        fragments.extend(self._parse_yaml_blocks(text, summary, records))
        fragments.extend(self._parse_csv_blocks(text, summary, records))
        fragments.extend(self._parse_key_values(text, summary, records))
        fragments.extend(self._parse_text_blocks(text, summary, records))

        return fragments, summary, records

    # Individual parsers -------------------------------------------------

    def _parse_json_fragments(
        self, text: str, summary: Dict[str, Any], records: List[Dict[str, Any]]
    ) -> List[ParsedFragment]:
        fragments: List[ParsedFragment] = []
        for match in self.JSON_PATTERN.finditer(text):
            candidate = match.group()
            try:
                parsed = json.loads(candidate)
            except json.JSONDecodeError:
                continue

            summary["json_fragments"] += 1
            fragments.append(
                ParsedFragment(
                    fragment_type="json",
                    start=match.start(),
                    end=match.end(),
                    preview=candidate[:80],
                    payload=parsed,
                )
            )
            records.append(
                {
                    "collection": "json_objects",
                    "data": parsed,
                    "source_range": [match.start(), match.end()],
                }
            )
        return fragments

    def _parse_html_fragments(
        self, text: str, summary: Dict[str, Any], records: List[Dict[str, Any]]
    ) -> List[ParsedFragment]:
        fragments: List[ParsedFragment] = []
        soup = BeautifulSoup(text, "lxml")
        tables = soup.find_all("table")
        for idx, table in enumerate(tables):
            headers = [th.get_text(strip=True) for th in table.find_all("th")]
            rows = []
            for tr in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if cells:
                    rows.append(cells)
            if not rows:
                continue

            df = self._table_to_dataframe(headers, rows)
            summary["html_tables"] += 1
            fragments.append(
                ParsedFragment(
                    fragment_type="html_table",
                    start=-1,
                    end=-1,
                    preview=str(table)[:80],
                    payload=df.to_dict(orient="records"),
                )
            )
            records.append(
                {
                    "collection": f"html_table_{idx}",
                    "data": df.to_dict(orient="records"),
                    "source_range": [-1, -1],
                }
            )
        return fragments

    def _parse_yaml_blocks(
        self, text: str, summary: Dict[str, Any], records: List[Dict[str, Any]]
    ) -> List[ParsedFragment]:
        fragments: List[ParsedFragment] = []
        for match in self.YAML_PATTERN.finditer(text):
            yaml_text = match.group("yaml").strip()
            if not yaml_text:
                continue
            try:
                payload = yaml.safe_load(yaml_text)
            except yaml.YAMLError:
                continue
            summary["yaml_blocks"] += 1
            fragments.append(
                ParsedFragment(
                    fragment_type="yaml",
                    start=match.start(),
                    end=match.end(),
                    preview=yaml_text[:80],
                    payload=payload,
                )
            )
            records.append(
                {
                    "collection": "yaml_blocks",
                    "data": payload,
                    "source_range": [match.start(), match.end()],
                }
            )
        return fragments

    def _parse_csv_blocks(
        self, text: str, summary: Dict[str, Any], records: List[Dict[str, Any]]
    ) -> List[ParsedFragment]:
        fragments: List[ParsedFragment] = []
        seen_spans: set[Tuple[int, int]] = set()
        for match in self.CSV_SECTION_PATTERN.finditer(text):
            span = match.span()
            if span in seen_spans:
                continue
            seen_spans.add(span)
            csv_text = f"{match.group('header')}\n{match.group('body')}"
            try:
                df = pd.read_csv(StringIO(csv_text))
            except Exception:
                continue
            summary["csv_blocks"] += 1
            fragments.append(
                ParsedFragment(
                    fragment_type="csv",
                    start=span[0],
                    end=span[1],
                    preview=csv_text.splitlines()[0][:80],
                    payload=df.to_dict(orient="records"),
                )
            )
            records.append(
                {
                    "collection": "csv_blocks",
                    "data": df.to_dict(orient="records"),
                    "source_range": [span[0], span[1]],
                }
            )
        return fragments

    def _parse_key_values(
        self, text: str, summary: Dict[str, Any], records: List[Dict[str, Any]]
    ) -> List[ParsedFragment]:
        fragments: List[ParsedFragment] = []
        kv_pairs = []
        for match in self.KV_PATTERN.finditer(text):
            key = match.group("key").strip().lower().replace(" ", "_")
            value = match.group("value").strip()
            kv_pairs.append({"key": key, "value": value})
        if kv_pairs:
            summary["kv_pairs"] += len(kv_pairs)
            fragments.append(
                ParsedFragment(
                    fragment_type="key_value_pairs",
                    start=-1,
                    end=-1,
                    preview=f"{kv_pairs[0]['key']}={kv_pairs[0]['value']}",
                    payload=kv_pairs,
                )
            )
            records.append(
                {
                    "collection": "key_values",
                    "data": {pair["key"]: pair["value"] for pair in kv_pairs},
                    "source_range": [-1, -1],
                }
            )
        return fragments

    def _parse_text_blocks(
        self, text: str, summary: Dict[str, Any], records: List[Dict[str, Any]]
    ) -> List[ParsedFragment]:
        # Remove recognized fragments to avoid duplication
        sanitized = self.JSON_PATTERN.sub("", text)
        sanitized = self.YAML_PATTERN.sub("", sanitized)
        blocks = [line.strip() for line in sanitized.splitlines() if line.strip()]
        fragments: List[ParsedFragment] = []
        if not blocks:
            return fragments
        summary["text_blocks"] += len(blocks)
        for idx, block in enumerate(blocks):
            fragments.append(
                ParsedFragment(
                    fragment_type="text",
                    start=-1,
                    end=-1,
                    preview=block[:80],
                    payload={"text": block},
                )
            )
            records.append(
                {
                    "collection": "text_blocks",
                    "data": {"text": block, "sequence": idx},
                    "source_range": [-1, -1],
                }
            )
        return fragments

    # Helpers ------------------------------------------------------------

    def _table_to_dataframe(self, headers: List[str], rows: List[List[str]]) -> pd.DataFrame:
        if headers and len(headers) == len(rows[0]):
            df = pd.DataFrame(rows[1:], columns=headers)
        else:
            df = pd.DataFrame(rows)
        return df


__all__ = ["ParserEngine", "ParsedFragment"]
