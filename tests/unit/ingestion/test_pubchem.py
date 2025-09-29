from __future__ import annotations

import gzip
import io
from pathlib import Path

import httpx

from open_molecule_data_pipeline.ingestion.common import (
    CheckpointManager,
    IngestionCheckpoint,
)
from open_molecule_data_pipeline.ingestion.pubchem import PubChemConfig, PubChemConnector


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


def _mock_pubchem_client(files: dict[str, bytes], base_url: str) -> httpx.Client:
    base = httpx.URL(base_url)
    listing_path = base.path
    if not listing_path.endswith("/"):
        listing_path = f"{listing_path}/"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method != "GET":  # pragma: no cover - defensive guard
            return httpx.Response(405)

        path = request.url.path
        if path.rstrip("/") == listing_path.rstrip("/"):
            links = "".join(
                f'<a href="{name}">{name}</a>' for name in sorted(files)
            )
            html = f"<html><body>{links}</body></html>"
            return httpx.Response(200, text=html)

        if not path.startswith(listing_path):
            return httpx.Response(404)

        filename = path[len(listing_path) :]
        if filename not in files:
            return httpx.Response(404)
        return httpx.Response(200, content=files[filename])

    transport = httpx.MockTransport(handler)
    return httpx.Client(transport=transport, timeout=5.0)


def test_pubchem_connector_batches_records(tmp_path: Path) -> None:
    file_a = _gzip_bytes(_sdf_entry("CID1", "C") + _sdf_entry("CID2", "CC"))
    file_b = _gzip_bytes(_sdf_entry("CID3", "CCC", PUBCHEM_IUPAC_NAME="Propane"))
    files = {"file_a.sdf.gz": file_a, "file_b.sdf.gz": file_b}
    base_url = "https://example.test/pubchem/Compound/CURRENT-Full/SDF/"

    manager = CheckpointManager(tmp_path)
    connector = PubChemConnector(
        config=PubChemConfig(name="pubchem", batch_size=2, base_url=base_url),
        checkpoint_manager=manager,
        client_factory=lambda: _mock_pubchem_client(files, base_url),
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
    base_url = "https://example.test/pubchem/Compound/CURRENT-Full/SDF/"

    manager = CheckpointManager(tmp_path)
    connector = PubChemConnector(
        config=PubChemConfig(name="pubchem", batch_size=2, base_url=base_url),
        checkpoint_manager=manager,
        client_factory=lambda: _mock_pubchem_client(files, base_url),
    )

    page_iter = connector.fetch_pages()
    first_page = next(page_iter)
    manager.store(
        "pubchem",
        IngestionCheckpoint(cursor=first_page.next_cursor or {}, batch_index=1, completed=False),
    )
    connector.close()

    resume_connector = PubChemConnector(
        config=PubChemConfig(name="pubchem", batch_size=2, base_url=base_url),
        checkpoint_manager=manager,
        client_factory=lambda: _mock_pubchem_client(files, base_url),
    )

    remaining_pages = list(resume_connector.fetch_pages())
    assert len(remaining_pages) == 1
    remaining = remaining_pages[0]
    assert [record.identifier for record in remaining.records] == ["CID3"]
    assert remaining.next_cursor is None
    resume_connector.close()
