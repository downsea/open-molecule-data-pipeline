from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from open_molecule_data_pipeline.ingestion.common import (
    CheckpointManager,
    IngestionCheckpoint,
)
from open_molecule_data_pipeline.ingestion.zinc import ZincConfig, ZincConnector


def _write_text_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _create_uri_manifest(path: Path, url: str) -> None:
    path.write_text(f"{url}\n", encoding="utf-8")


def test_fetch_pages_uses_existing_archives(tmp_path: Path) -> None:
    download_dir = tmp_path / "downloads"
    archive_path = download_dir / "2D" / "AA" / "AAAA.txt"
    _write_text_lines(
        archive_path,
        [
            "C\tZINC00000001",
            "CC\tZINC00000002",
            "CCC\tZINC00000003",
        ],
    )

    manifest_path = tmp_path / "zinc.uri"
    url = "https://irwinlab2.ucsf.edu/2D/AA/AAAA.txt"
    _create_uri_manifest(manifest_path, url)

    config = ZincConfig(
        name="zinc",
        uri_file=manifest_path,
        download_dir=download_dir,
        batch_size=2,
    )
    checkpoint_manager = CheckpointManager(tmp_path / "checkpoints")

    called: dict[str, Any] = {}

    def fake_downloader(*args: object, **kwargs: object) -> None:
        called["invoked"] = True

    connector = ZincConnector(
        config=config,
        checkpoint_manager=checkpoint_manager,
        aria2_downloader=fake_downloader,
    )

    pages = list(connector.fetch_pages())

    assert "invoked" not in called
    assert len(pages) == 2
    first_page, second_page = pages
    assert [record.identifier for record in first_page.records] == ["ZINC00000001", "ZINC00000002"]
    assert [record.identifier for record in second_page.records] == ["ZINC00000003"]
    assert second_page.next_cursor is None


def test_missing_archive_triggers_download(tmp_path: Path) -> None:
    manifest_path = tmp_path / "zinc.uri"
    url = "https://irwinlab2.ucsf.edu/2D/AA/AAAA.txt"
    _create_uri_manifest(manifest_path, url)

    downloaded: dict[str, Any] = {}

    def fake_downloader(url: str, output_path: Path, **kwargs: object) -> None:
        downloaded["url"] = url
        downloaded["kwargs"] = kwargs
        _write_text_lines(
            output_path,
            [
                "C\tZINC00000001",
                "CC\tZINC00000002",
            ],
        )

    config = ZincConfig(
        name="zinc",
        uri_file=manifest_path,
        download_dir=tmp_path / "downloads",
        batch_size=2,
        download_missing=True,
        username="user",
        password="pass",
    )
    checkpoint_manager = CheckpointManager(tmp_path / "checkpoints")
    connector = ZincConnector(
        config=config,
        checkpoint_manager=checkpoint_manager,
        aria2_downloader=fake_downloader,
    )

    pages = list(connector.fetch_pages())

    assert downloaded["url"].endswith("AA/AAAA.txt")
    assert downloaded["kwargs"]["username"] == "user"
    assert downloaded["kwargs"]["password"] == "pass"
    assert len(pages) == 1
    assert [record.identifier for record in pages[0].records] == ["ZINC00000001", "ZINC00000002"]


def test_missing_archive_without_download(tmp_path: Path) -> None:
    manifest_path = tmp_path / "zinc.uri"
    url = "https://irwinlab2.ucsf.edu/2D/AA/AAAA.txt"
    _create_uri_manifest(manifest_path, url)

    config = ZincConfig(
        name="zinc",
        uri_file=manifest_path,
        download_dir=tmp_path / "downloads",
    )
    checkpoint_manager = CheckpointManager(tmp_path / "checkpoints")
    connector = ZincConnector(config=config, checkpoint_manager=checkpoint_manager)

    with pytest.raises(FileNotFoundError):
        list(connector.fetch_pages())


def test_fetch_pages_respects_checkpoint(tmp_path: Path) -> None:
    download_dir = tmp_path / "downloads"
    archive_path = download_dir / "2D" / "AA" / "AAAA.txt"
    _write_text_lines(
        archive_path,
        [
            "C\tZINC00000001",
            "CC\tZINC00000002",
            "CCC\tZINC00000003",
        ],
    )

    manifest_path = tmp_path / "zinc.uri"
    url = "https://irwinlab2.ucsf.edu/2D/AA/AAAA.txt"
    _create_uri_manifest(manifest_path, url)

    config = ZincConfig(
        name="zinc",
        uri_file=manifest_path,
        download_dir=download_dir,
        batch_size=2,
    )
    checkpoint_manager = CheckpointManager(tmp_path / "checkpoints")
    checkpoint_manager.store(
        "zinc",
        IngestionCheckpoint(cursor={"entry_index": 0, "line_offset": 2}, batch_index=1),
    )

    connector = ZincConnector(config=config, checkpoint_manager=checkpoint_manager)
    pages = list(connector.fetch_pages())

    assert len(pages) == 1
    assert [record.identifier for record in pages[0].records] == ["ZINC00000003"]
