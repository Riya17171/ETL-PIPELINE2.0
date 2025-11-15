"""Flask API implementing the dynamic ETL pipeline contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS

from dynamic_etl import DynamicETLPipeline, LLMQueryEngine, MongoStorage

app = Flask(__name__)
CORS(app)

storage = MongoStorage()
pipeline = DynamicETLPipeline(storage=storage)
llm_engine = LLMQueryEngine()


@app.route("/upload", methods=["POST"])
def upload():
    """Handle file uploads and trigger ETL processing."""
    if "file" not in request.files:
        return jsonify({"error": "file field missing"}), 400

    file = request.files["file"]
    source_id = request.form.get("source_id", "default_source")
    version = request.form.get("version")
    version = int(version) if version else None

    try:
        result = pipeline.process(file.filename, source_id, binary=file.read(), version=version)
    except Exception as exc:  # pragma: no cover - runtime diagnostics
        return jsonify({"error": str(exc)}), 500

    response = {
        "status": "ok",
        "source_id": result.source_id,
        "file_id": result.file_id,
        "schema_id": result.schema["schema_id"],
        "version": result.version,
        "parsed_fragments_summary": result.summary,
    }
    return jsonify(response)


@app.route("/schema", methods=["GET"])
def schema():
    source_id = request.args.get("source_id")
    if not source_id:
        return jsonify({"error": "source_id is required"}), 400
    version = request.args.get("version")
    version = int(version) if version else None
    doc = storage.get_schema(source_id, version)
    if not doc:
        return jsonify({"error": "schema not found"}), 404
    doc.pop("_id", None)
    return jsonify(doc)


@app.route("/schema/history", methods=["GET"])
def schema_history():
    source_id = request.args.get("source_id")
    if not source_id:
        return jsonify({"error": "source_id is required"}), 400
    history = storage.get_schema_history(source_id)
    for entry in history:
        entry.pop("_id", None)
    return jsonify({"history": history})


@app.route("/schema/download", methods=["GET"])
def schema_download():
    source_id = request.args.get("source_id")
    version = request.args.get("version")
    if not source_id:
        return jsonify({"error": "source_id is required"}), 400
    version = int(version) if version else storage.latest_version(source_id)
    schema_file = Path("outputs/schemas") / source_id / f"schema_v{version}.json"
    if not schema_file.exists():
        return jsonify({"error": "schema file not found"}), 404
    return send_file(schema_file, mimetype="application/json", as_attachment=True, download_name=schema_file.name)


@app.route("/query", methods=["POST"])
def query():
    payload: Dict[str, Any] = request.get_json(force=True)
    source_id = payload.get("source_id")
    nl_query = payload.get("nl_query", "")
    if not source_id:
        return jsonify({"error": "source_id is required"}), 400
    if not nl_query:
        return jsonify({"error": "nl_query is required"}), 400

    schema_doc = storage.get_schema(source_id)
    if not schema_doc:
        return jsonify({"error": "schema not found for source"}), 404

    translation = llm_engine.translate(nl_query, schema_doc)
    results = execute_translated_query(source_id, translation.translated_query)

    query_id = storage.store_query_result(
        source_id=source_id,
        nl_query=nl_query,
        translated_query=translation.translated_query,
        results=results,
    )

    return jsonify(
        {
            "query_id": query_id,
            "translated_query": translation.translated_query,
            "raw_response": translation.raw_response,
            "results": results,
        }
    )


def execute_translated_query(source_id: str, translated_query: Dict[str, Any]):
    def _qualify_filter(raw_filter: Dict[str, Any]) -> Dict[str, Any]:
        qualified = {}
        for key, value in raw_filter.items():
            key = key.replace("$.", "")
            qualified[f"data.{key}"] = value
        return qualified

    if "aggregate" in translated_query:
        pipeline_stages = []
        for stage in translated_query["aggregate"]:
            if "$match" in stage:
                stage = {"$match": _qualify_filter(stage["$match"])}
            elif "$group" in stage:
                group_stage = stage["$group"].copy()
                for key, value in list(group_stage.items()):
                    if isinstance(value, dict) and "$avg" in value:
                        group_stage[key] = {"$avg": f"$data.{value['$avg'].lstrip('$').replace('$.', '')}"}
                stage = {"$group": group_stage}
            pipeline_stages.append(stage)
        cursor = storage.records.aggregate(
            [{"$match": {"source_id": source_id}}] + pipeline_stages
        )
        return [_normalize_mongo_document(doc) for doc in cursor]

    find_filter = _qualify_filter(translated_query.get("find", {}))
    projection = translated_query.get("projection")
    cursor = storage.records.find({"source_id": source_id, **find_filter}, projection)
    return [_normalize_mongo_document(doc) for doc in cursor]


def _normalize_mongo_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc = dict(doc)
    identifier = doc.get("_id")
    if identifier is not None:
        doc["_id"] = str(identifier)
    return doc


@app.route("/records", methods=["GET"])
def records():
    source_id = request.args.get("source_id")
    if not source_id:
        return jsonify({"error": "source_id is required"}), 400
    query_id = request.args.get("query_id")
    if query_id:
        record = storage.get_query_result(query_id)
        if not record:
            return jsonify({"error": "query result not found"}), 404
        record.pop("_id", None)
        return jsonify(record)
    version = request.args.get("version")
    version = int(version) if version else None
    rows = storage.get_records(source_id, version)
    for row in rows:
        row.pop("_id", None)
    return jsonify({"records": rows})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=False)
