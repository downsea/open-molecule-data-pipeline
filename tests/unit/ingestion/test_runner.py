from __future__ import annotations

import gzip
import hashlib
import io
import json
from pathlib import Path
from typing import Any

import pytest

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


def _write_index(path: Path, filenames: list[str]) -> None:
    links = "".join(f'<a href="{name}">{name}</a>\n' for name in filenames)
    path.write_text(f"<html><body>{links}</body></html>")


def test_run_ingestion_writes_batches_and_checkpoints(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base_url = "https://example.test/pubchem/"
    index_file = tmp_path / "index.html"
    filenames = ["chunk_a.sdf.gz", "chunk_a.sdf.gz.md5", "chunk_b.sdf.gz", "chunk_b.sdf.gz.md5"]
    _write_index(index_file, filenames)

    payload_a = _gzip_bytes(_sdf_entry("CID1", "C") + _sdf_entry("CID2", "CC"))
    payload_b = _gzip_bytes(_sdf_entry("CID3", "CCC"))
    fixtures: dict[str, bytes] = {
        f"{base_url}chunk_a.sdf.gz": payload_a,
        f"{base_url}chunk_a.sdf.gz.md5": hashlib.md5(payload_a).hexdigest().encode("utf-8")
        + b"  chunk_a.sdf.gz\n",
        f"{base_url}chunk_b.sdf.gz": payload_b,
        f"{base_url}chunk_b.sdf.gz.md5": hashlib.md5(payload_b).hexdigest().encode("utf-8")
        + b"  chunk_b.sdf.gz\n",
    }

    def fake_downloader(url: str, output_path: Path, **kwargs: Any) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = fixtures[url]
        if url.endswith(".md5"):
            output_path.write_text(payload.decode("utf-8"))
        else:
            output_path.write_bytes(payload)

    monkeypatch.setattr(
        "open_molecule_data_pipeline.ingestion.pubchem.download_with_aria2",
        fake_downloader,
    )

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
                    "index_file": index_file,
                    "download_dir": tmp_path / "downloads",
                    "base_url": base_url,
                },
            )
        ],
    )

    run_ingestion(config)

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

    # Running again should read from checkpoint without additional output
    run_ingestion(config)
