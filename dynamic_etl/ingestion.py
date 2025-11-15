"""File ingestion utilities for the dynamic ETL pipeline."""

from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Tuple

import pdfplumber


class FileIngestor:
    """Read raw content from supported file formats."""

    SUPPORTED_TYPES = {".txt", ".md", ".markdown", ".pdf"}

    def __init__(self, base_input_dir: Path | str):
        self.base_input_dir = Path(base_input_dir)
        self.base_input_dir.mkdir(parents=True, exist_ok=True)

    def read(self, filename: str, binary: bytes | None = None) -> Tuple[str, bytes | None]:
        """Return textual content for the provided filename.

        Args:
            filename: Name of the file on disk or provided via upload.
            binary: Optional binary content (used for in-memory uploads).

        Returns:
            A tuple containing the decoded text and original binary payload.
        """
        suffix = Path(filename).suffix.lower()
        if suffix not in self.SUPPORTED_TYPES:
            raise ValueError(
                f"Unsupported file type '{suffix}'. Supported types: {sorted(self.SUPPORTED_TYPES)}"
            )

        if binary is not None:
            payload = binary
        else:
            file_path = self.base_input_dir / filename
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            payload = file_path.read_bytes()

        if suffix == ".pdf":
            return self._read_pdf(payload), payload

        # Default text-based reading
        text = self._decode_text(payload)
        return text, payload

    def _decode_text(self, payload: bytes) -> str:
        """Decode bytes payload using UTF-8 with fallback to Latin-1."""
        try:
            return payload.decode("utf-8")
        except UnicodeDecodeError:
            return payload.decode("latin-1", errors="replace")

    def _read_pdf(self, payload: bytes) -> str:
        """Extract text from a PDF payload using pdfplumber."""
        with pdfplumber.open(io.BytesIO(payload)) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
        return "\n".join(pages)


__all__ = ["FileIngestor"]
