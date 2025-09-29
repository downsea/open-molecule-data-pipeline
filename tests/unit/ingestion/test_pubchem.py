from __future__ import annotations

import gzip
import io
from collections.abc import Callable
from pathlib import Path

from open_molecule_data_pipeline.ingestion.common import (
    CheckpointManager,
    IngestionCheckpoint,
)
from open_molecule_data_pipeline.ingestion.pubchem import PubChemConfig, PubChemConnector


class FakeFTP:
    """Minimal in-memory FTP server for testing."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files
        self.cwd_path: str | None = None
        self.closed = False

    def cwd(self, path: str) -> None:  # noqa: D401 - required by ftplib interface
        self.cwd_path = path

    def nlst(self) -> list[str]:
        return sorted(self._files)

    def retrbinary(self, cmd: str, callback: Callable[[bytes], object]) -> None:
        _, filename = cmd.split(maxsplit=1)
        data = self._files[filename]
        callback(data)

    def quit(self) -> None:
        self.closed = True


def _gzip_bytes(payload: str) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        gz.write(payload.encode("utf-8"))
    return buffer.getvalue()


def _sdf_entry(cid: str, smiles: str, **metadata: str) -> str:
    lines = [
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
    ]
    for key, value in metadata.items():
        lines.extend([f">  <{key}>", value, ""])
    lines.append("$$$$")
    return "\n".join(lines) + "\n"


def test_pubchem_connector_batches_records(tmp_path: Path) -> None:
    file_a = _gzip_bytes(_sdf_entry("CID1", "C") + _sdf_entry("CID2", "CC"))
    file_b = _gzip_bytes(_sdf_entry("CID3", "CCC", PUBCHEM_IUPAC_NAME="Propane"))
    files = {"file_a.sdf.gz": file_a, "file_b.sdf.gz": file_b}

    manager = CheckpointManager(tmp_path)
    connector = PubChemConnector(
        config=PubChemConfig(name="pubchem", batch_size=2, ftp_directory="."),
        checkpoint_manager=manager,
        ftp_factory=lambda: FakeFTP(files),
    )

    pages = list(connector.fetch_pages())
    assert len(pages) == 2

    first, second = pages
    assert [record.identifier for record in first.records] == ["CID1", "CID2"]
    assert isinstance(first.next_cursor, dict)
    assert first.next_cursor["record_offset"] == 2

    assert [record.identifier for record in second.records] == ["CID3"]
    assert second.next_cursor is None

    connector.close()


def test_pubchem_connector_resumes_from_checkpoint(tmp_path: Path) -> None:
    sdf_payload = _sdf_entry("CID1", "C") + _sdf_entry("CID2", "CC") + _sdf_entry("CID3", "CCC")
    files = {"chunk.sdf.gz": _gzip_bytes(sdf_payload)}

    manager = CheckpointManager(tmp_path)
    connector = PubChemConnector(
        config=PubChemConfig(name="pubchem", batch_size=2, ftp_directory="."),
        checkpoint_manager=manager,
        ftp_factory=lambda: FakeFTP(files),
    )

    page_iter = connector.fetch_pages()
    first_page = next(page_iter)
    manager.store(
        "pubchem",
        IngestionCheckpoint(cursor=first_page.next_cursor or {}, batch_index=1, completed=False),
    )
    connector.close()

    resume_connector = PubChemConnector(
        config=PubChemConfig(name="pubchem", batch_size=2, ftp_directory="."),
        checkpoint_manager=manager,
        ftp_factory=lambda: FakeFTP(files),
    )

    remaining_pages = list(resume_connector.fetch_pages())
    assert len(remaining_pages) == 1
    remaining = remaining_pages[0]
    assert [record.identifier for record in remaining.records] == ["CID3"]
    assert remaining.next_cursor is None
    resume_connector.close()
