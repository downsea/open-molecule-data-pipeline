from __future__ import annotations

import gzip
import hashlib
import io
from pathlib import Path
from typing import Any

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

def _write_link_file(path: Path, urls: list[str]) -> None:
    path.write_text("\n".join(urls) + "\n")

def _build_downloader(fixtures: dict[str, bytes], checksum_suffix: str = ".md5"):
    calls: list[dict[str, Any]] = []

    def downloader(url: str, output_path: Path, **kwargs: Any) -> None:
        calls.append({"url": url, "kwargs": kwargs, "output": output_path})
        if url.endswith(checksum_suffix):
            value = fixtures[url].decode("utf-8")
            output_path.write_text(value)
        else:
            output_path.write_bytes(fixtures[url])

    return downloader, calls


def test_pubchem_connector_batches_records(tmp_path: Path) -> None:
    sdf_a = _gzip_bytes(_sdf_entry("CID1", "C") + _sdf_entry("CID2", "CC"))
    sdf_b = _gzip_bytes(_sdf_entry("CID3", "CCC", PUBCHEM_IUPAC_NAME="Propane"))
    md5_a = hashlib.md5(sdf_a).hexdigest().encode("utf-8")
    md5_b = hashlib.md5(sdf_b).hexdigest().encode("utf-8")

    urls = [
        "https://example.test/pubchem/Compound_A.sdf.gz",
        "https://example.test/pubchem/Compound_B.sdf.gz",
    ]
    link_file = tmp_path / "links.txt"
    _write_link_file(link_file, urls)

    fixtures = {
        f"{url}": payload
        for url, payload in {
            urls[0]: sdf_a,
            f"{urls[0]}.md5": md5_a + b"  Compound_A.sdf.gz\n",
            urls[1]: sdf_b,
            f"{urls[1]}.md5": md5_b + b"  Compound_B.sdf.gz\n",
        }.items()
    }

    downloader, calls = _build_downloader(fixtures)

    manager = CheckpointManager(tmp_path / "checkpoints")
    connector = PubChemConnector(
        config=PubChemConfig(
            name="pubchem",
            batch_size=2,
            link_file=link_file,
            download_dir=tmp_path / "downloads",
        ),
        checkpoint_manager=manager,
        aria2_downloader=downloader,
    )

    pages = list(connector.fetch_pages())

    assert len(pages) == 2
    first, second = pages
    assert [record.identifier for record in first.records] == ["CID1", "CID2"]
    assert isinstance(first.next_cursor, dict)
    assert first.next_cursor["record_offset"] == 2

    assert [record.identifier for record in second.records] == ["CID3"]
    assert second.next_cursor is None

    checksum_urls = [call["url"] for call in calls if call["url"].endswith(".md5")]
    assert len(checksum_urls) == 2


def test_pubchem_connector_resumes_from_checkpoint(tmp_path: Path) -> None:
    payload = _gzip_bytes(
        _sdf_entry("CID1", "C")
        + _sdf_entry("CID2", "CC")
        + _sdf_entry("CID3", "CCC")
    )
    md5_payload = hashlib.md5(payload).hexdigest().encode("utf-8")

    url = "https://example.test/pubchem/Chunk.sdf.gz"
    link_file = tmp_path / "links.txt"
    _write_link_file(link_file, [url])

    fixtures = {
        url: payload,
        f"{url}.md5": md5_payload + b"  Chunk.sdf.gz\n",
    }

    downloader, calls = _build_downloader(fixtures)

    manager = CheckpointManager(tmp_path / "checkpoints")
    connector = PubChemConnector(
        config=PubChemConfig(
            name="pubchem",
            batch_size=2,
            link_file=link_file,
            download_dir=tmp_path / "downloads",

        ),
        checkpoint_manager=manager,
        aria2_downloader=downloader,
    )

    first_page = next(connector.fetch_pages())
    manager.store(
        "pubchem",
        IngestionCheckpoint(cursor=first_page.next_cursor or {}, batch_index=1, completed=False),
    )

    remaining_pages = list(connector.fetch_pages())
    assert len(remaining_pages) == 1
    remaining = remaining_pages[0]
    assert [record.identifier for record in remaining.records] == ["CID3"]
    assert remaining.next_cursor is None

    checksum_calls = [call for call in calls if call["url"].endswith(".md5")]
    assert checksum_calls
