from __future__ import annotations

import gzip
import io
import json
from pathlib import Path

from open_molecule_data_pipeline.ingestion.runner import (
    IngestionJobConfig,
    SourceDefinition,
    run_ingestion,
)


class FakeFTP:
    def __init__(self, files: dict[str, bytes], calls: dict[str, int]) -> None:
        self._files = files
        self._calls = calls

    def cwd(self, path: str) -> None:  # noqa: D401 - required by ftplib interface
        self._cwd = path

    def nlst(self) -> list[str]:
        return sorted(self._files)

    def retrbinary(self, cmd: str, callback) -> None:
        self._calls["retr"] = self._calls.get("retr", 0) + 1
        _, filename = cmd.split(maxsplit=1)
        callback(self._files[filename])

    def quit(self) -> None:
        self._calls["quit"] = self._calls.get("quit", 0) + 1


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


def test_run_ingestion_writes_batches_and_checkpoints(tmp_path: Path) -> None:
    files = {
        "chunk_a.sdf.gz": _gzip_bytes(_sdf_entry("CID1", "C") + _sdf_entry("CID2", "CC")),
        "chunk_b.sdf.gz": _gzip_bytes(_sdf_entry("CID3", "CCC")),
    }
    calls: dict[str, int] = {}

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
                    "ftp_directory": ".",
                },
            )
        ],
    )

    def ftp_factory() -> FakeFTP:
        calls["factory"] = calls.get("factory", 0) + 1
        return FakeFTP(files, calls)

    run_ingestion(config, client_factories={"pubchem": ftp_factory})

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

    run_ingestion(config, client_factories={"pubchem": ftp_factory})
    assert calls["factory"] == 1
