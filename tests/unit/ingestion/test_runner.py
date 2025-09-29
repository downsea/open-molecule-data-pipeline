from __future__ import annotations

import json
from pathlib import Path

import httpx
import orjson

from open_molecule_data_pipeline.ingestion.runner import (
    IngestionJobConfig,
    SourceDefinition,
    run_ingestion,
)



def test_run_ingestion_writes_batches_and_checkpoints(tmp_path: Path) -> None:
    calls: dict[str, int] = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        if cursor is None:
            calls["count"] += 1
            payload = {
                "records": [
                    {"id": "CID1", "smiles": "C", "inchi_key": "KEY1"},
                    {"id": "CID2", "smiles": "CC"},
                ],
                "next": {"cursor": "token-1"},
            }
        elif cursor == "token-1":
            calls["count"] += 1
            payload = {
                "records": [
                    {"id": "CID3", "smiles": "CCC"},
                ],
                "next": None,
            }
        else:  # pragma: no cover - defensive
            payload = {"records": [], "next": None}
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)

    config = IngestionJobConfig(
        output_dir=tmp_path / "raw",
        checkpoint_dir=tmp_path / "checkpoints",
        batch_size=2,
        concurrency=1,
        compress_output=False,
        sources=[
            SourceDefinition(
                type="pubchem",
                name="pubchem",
                options={
                    "base_url": "https://mock.api",
                    "endpoint": "records",
                    "batch_param": "limit",
                    "cursor_param": "cursor",
                    "records_path": ["records"],
                    "next_cursor_path": ["next"],
                    "id_field": "id",
                    "smiles_field": "smiles",
                },
            )
        ],
    )

    def client_factory(headers: dict[str, str]) -> httpx.Client:  # noqa: ARG001
        return httpx.Client(transport=transport)

    run_ingestion(config, client_factories={"pubchem": client_factory})

    output_dir = tmp_path / "raw" / "pubchem"
    first_batch = output_dir / "pubchem-batch-000001.jsonl"
    second_batch = output_dir / "pubchem-batch-000002.jsonl"

    assert first_batch.exists()
    with first_batch.open("r", encoding="utf-8") as fh:
        first_lines = [json.loads(line) for line in fh]
    assert [record["identifier"] for record in first_lines] == ["CID1", "CID2"]

    assert second_batch.exists()
    with second_batch.open("r", encoding="utf-8") as fh:
        second_lines = [json.loads(line) for line in fh]
    assert [record["identifier"] for record in second_lines] == ["CID3"]

    checkpoint_path = tmp_path / "checkpoints" / "ingestion" / "pubchem.json"
    checkpoint = orjson.loads(checkpoint_path.read_bytes())
    assert checkpoint["batch_index"] == 2
    assert checkpoint["completed"] is True

    # Running again should not trigger additional HTTP calls because the source is complete.
    run_ingestion(config, client_factories={"pubchem": client_factory})
    assert calls["count"] == 2
