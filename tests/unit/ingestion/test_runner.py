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


def _write_link_file(path: Path, urls: list[str]) -> None:
    path.write_text("\n".join(urls) + "\n")


def test_run_ingestion_writes_batches_and_checkpoints(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    urls = [
        "https://example.test/pubchem/chunk_a.sdf.gz",
        "https://example.test/pubchem/chunk_b.sdf.gz",
    ]
    link_file = tmp_path / "links.txt"
    _write_link_file(link_file, urls)

    payload_a = _gzip_bytes(_sdf_entry("CID1", "C") + _sdf_entry("CID2", "CC"))
    payload_b = _gzip_bytes(_sdf_entry("CID3", "CCC"))
    fixtures: dict[str, bytes] = {
        urls[0]: payload_a,
        f"{urls[0]}.md5": hashlib.md5(payload_a).hexdigest().encode("utf-8")
        + b"  chunk_a.sdf.gz\n",
        urls[1]: payload_b,
        f"{urls[1]}.md5": hashlib.md5(payload_b).hexdigest().encode("utf-8")
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
                    "link_file": link_file,
                    "download_dir": tmp_path / "downloads",
                },
            )
        ],
    )

    run_ingestion(config, mode="download")

    downloads_dir = tmp_path / "downloads"
    assert (downloads_dir / "chunk_a.sdf.gz").exists()
    assert (downloads_dir / "chunk_b.sdf.gz").exists()

    checkpoint_path = tmp_path / "checkpoints" / "ingestion-download" / "pubchem.json"
    assert checkpoint_path.exists()
    checkpoint = json.loads(checkpoint_path.read_text())
    assert checkpoint["batch_index"] == 0
    assert checkpoint["completed"] is True

    report_path = tmp_path / "raw" / "raw-data-report.md"
    assert report_path.exists()
    report_contents = report_path.read_text(encoding="utf-8")
    assert "# Raw Data Download Report" in report_contents
    assert "## pubchem" in report_contents
    assert "| pubchem | pubchem | yes | 0 | 0 | 0 |" in report_contents
    assert str(tmp_path / "downloads").replace("\\", "/") in report_contents

    # Running again should read from checkpoint without additional output
    run_ingestion(config, mode="download")


def test_parse_ingestion_emits_batches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    urls = [
        "https://example.test/pubchem/chunk_a.sdf.gz",
        "https://example.test/pubchem/chunk_b.sdf.gz",
    ]
    link_file = tmp_path / "links.txt"
    _write_link_file(link_file, urls)

    payload_a = _gzip_bytes(_sdf_entry("CID1", "C") + _sdf_entry("CID2", "CC"))
    payload_b = _gzip_bytes(_sdf_entry("CID3", "CCC"))

    downloads_dir = tmp_path / "downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    (downloads_dir / "chunk_a.sdf.gz").write_bytes(payload_a)
    (downloads_dir / "chunk_b.sdf.gz").write_bytes(payload_b)

    def fail_downloader(*args: object, **kwargs: object) -> None:  # pragma: no cover
        raise AssertionError("download invoked during parse phase")

    monkeypatch.setattr(
        "open_molecule_data_pipeline.ingestion.pubchem.download_with_aria2",
        fail_downloader,
    )

    config = IngestionJobConfig(
        output_dir=tmp_path / "processed",
        checkpoint_dir=tmp_path / "checkpoints",
        batch_size=2,
        concurrency=1,
        compress_output=False,
        sources=[
            SourceDefinition(
                type="pubchem",
                name="pubchem",
                options={
                    "link_file": link_file,
                    "download_dir": downloads_dir,
                },
            )
        ],
    )

    run_ingestion(config, mode="parse")

    output_dir = tmp_path / "processed" / "pubchem"
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

    checkpoint_path = tmp_path / "checkpoints" / "ingestion-parse" / "pubchem.json"
    assert checkpoint_path.exists()
    checkpoint = json.loads(checkpoint_path.read_text())
    assert checkpoint["batch_index"] == 2
    assert checkpoint["completed"] is True

    report_path = tmp_path / "processed" / "raw-data-report.md"
    assert report_path.exists()
    report_contents = report_path.read_text(encoding="utf-8")
    assert "## pubchem" in report_contents
    assert "| pubchem | pubchem | yes | 2 | 2 | 3 |" in report_contents
