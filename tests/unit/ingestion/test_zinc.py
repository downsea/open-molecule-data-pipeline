from __future__ import annotations

import gzip
from pathlib import Path

import pytest

from ingestion.common import CheckpointManager, IngestionCheckpoint
from ingestion.zinc import ZincConfig, ZincConnector


def _write_gzip_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for line in lines:
            handle.write(line)
            handle.write("\n")


def _create_wget_script(path: Path, relative_path: str) -> None:
    path.write_text(
        "mkdir -pv H04 && wget https://files.docking.org/zinc22/2d/H04/H04M000.smi.gz"
        f" -O {relative_path}\n"
    )


def test_fetch_pages_from_wget_script(tmp_path: Path) -> None:
    download_dir = tmp_path / "downloads"
    archive_path = download_dir / "H04" / "H04M000.smi.gz"
    _write_gzip_lines(
        archive_path,
        [
            "C\tZINC00000001",
            "CC\tZINC00000002",
            "CCC\tZINC00000003",
        ],
    )

    script_path = tmp_path / "zinc.wget"
    _create_wget_script(script_path, "H04/H04M000.smi.gz")

    config = ZincConfig(
        name="zinc",
        wget_file=script_path,
        download_dir=download_dir,
        batch_size=2,
    )
    checkpoint_manager = CheckpointManager(tmp_path / "checkpoints")
    connector = ZincConnector(config=config, checkpoint_manager=checkpoint_manager)

    pages = list(connector.fetch_pages())

    assert len(pages) == 2
    first_page, second_page = pages

    assert len(first_page.records) == 2
    assert first_page.records[0].identifier == "ZINC00000001"
    assert first_page.records[0].smiles == "C"
    assert first_page.records[0].metadata["source_file"] == "H04/H04M000.smi.gz"

    assert len(second_page.records) == 1
    assert second_page.next_cursor is None


def test_fetch_pages_respects_checkpoint(tmp_path: Path) -> None:
    download_dir = tmp_path / "downloads"
    archive_path = download_dir / "H04" / "H04M000.smi.gz"
    _write_gzip_lines(
        archive_path,
        [
            "C\tZINC00000001",
            "CC\tZINC00000002",
            "CCC\tZINC00000003",
        ],
    )

    script_path = tmp_path / "zinc.wget"
    _create_wget_script(script_path, "H04/H04M000.smi.gz")

    config = ZincConfig(
        name="zinc",
        wget_file=script_path,
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
    assert len(pages[0].records) == 1
    assert pages[0].records[0].identifier == "ZINC00000003"


def test_missing_archive_raises_without_download(tmp_path: Path) -> None:
    download_dir = tmp_path / "downloads"
    script_path = tmp_path / "zinc.wget"
    _create_wget_script(script_path, "H04/H04M000.smi.gz")

    config = ZincConfig(
        name="zinc",
        wget_file=script_path,
        download_dir=download_dir,
    )
    checkpoint_manager = CheckpointManager(tmp_path / "checkpoints")
    connector = ZincConnector(config=config, checkpoint_manager=checkpoint_manager)

    with pytest.raises(FileNotFoundError):
        list(connector.fetch_pages())
