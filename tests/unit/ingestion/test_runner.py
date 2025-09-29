from __future__ import annotations

import gzip
import io
import json
from pathlib import Path

import httpx

from open_molecule_data_pipeline.ingestion.runner import (
    IngestionJobConfig,
    SourceDefinition,
    run_ingestion,
)


def _gzip_bytes(payload: str) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        gz.write(payload.encode("utf-8"))
    return buffer.getvalue()


def _sdf_entry(cid: str, smiles: str) -> str:
    return "\n".join(
        [
            "PubChem",
            "  -OEChem-",
            "",
            "  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0",
            "M  END",
            ">  <PUBCHEM_COMPOUND_CID>",
            cid,
            "",
            ">  <PUBCHEM_OPENEYE_ISO_SMILES>",
            smiles,
            "",
            "$$$$",
            "",
        ]
    )


def _mock_pubchem_client(
    files: dict[str, bytes], base_url: str, calls: dict[str, int]
) -> httpx.Client:
    base = httpx.URL(base_url)
    listing_path = base.path
    if not listing_path.endswith("/"):
        listing_path = f"{listing_path}/"

    def handler(request: httpx.Request) -> httpx.Response:
        calls["requests"] = calls.get("requests", 0) + 1
        if request.method != "GET":  # pragma: no cover - defensive guard
            return httpx.Response(405)

        path = request.url.path
        if path.rstrip("/") == listing_path.rstrip("/"):
            links = "".join(
                f'<a href="{name}">{name}</a>' for name in sorted(files)
            )
            return httpx.Response(200, text=f"<html><body>{links}</body></html>")

        if not path.startswith(listing_path):
            return httpx.Response(404)

        filename = path[len(listing_path) :]
        if filename not in files:
            return httpx.Response(404)
        return httpx.Response(200, content=files[filename])

    transport = httpx.MockTransport(handler)
    return httpx.Client(transport=transport, timeout=5.0)


def test_run_ingestion_writes_batches_and_checkpoints(tmp_path: Path) -> None:
    files = {
        "chunk_a.sdf.gz": _gzip_bytes(_sdf_entry("CID1", "C") + _sdf_entry("CID2", "CC")),
        "chunk_b.sdf.gz": _gzip_bytes(_sdf_entry("CID3", "CCC")),
    }
    calls: dict[str, int] = {}
    base_url = "https://example.test/pubchem/Compound/CURRENT-Full/SDF/"

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
                    "base_url": base_url,
                },
            )
        ],
    )

    def client_factory() -> httpx.Client:
        calls["factory"] = calls.get("factory", 0) + 1
        return _mock_pubchem_client(files, base_url, calls)

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
    assert checkpoint_path.exists()
    checkpoint = json.loads(checkpoint_path.read_text())
    assert checkpoint["batch_index"] == 2
    assert checkpoint["completed"] is True

    run_ingestion(config, client_factories={"pubchem": client_factory})
    assert calls["factory"] == 1
