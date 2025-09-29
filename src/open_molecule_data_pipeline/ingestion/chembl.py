"""ChEMBL ingestion connector using bulk SDF downloads."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Mapping

from pydantic import Field

from .aria2 import Aria2Options, download_with_aria2
from .common import (
    BaseConnector,
    CheckpointManager,
    IngestionPage,
    MoleculeRecord,
    SourceConfig,
)
from .sdf import iter_sdf_records
from ..logging_utils import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class _ChEMBLEntry:
    filename: str
    url: str


class ChEMBLConfig(SourceConfig):
    """Configuration for downloading ChEMBL SDF exports."""

    link_file: Path = Field(description="Path to the file containing ChEMBL SDF URLs.")
    download_dir: Path | None = Field(
        default=None,
        description="Directory where downloaded ChEMBL archives are stored.",
    )
    identifier_tag: str = Field(
        default="ChEMBL_ID",
        description="Identifier field retained for backwards compatibility.",
    )
    smiles_tag: str = Field(
        default="CANONICAL_SMILES",
        description="SMILES field retained for backwards compatibility.",
    )
    metadata_tags: list[str] = Field(
        default_factory=list,
        description="Metadata fields retained for backwards compatibility.",
    )
    aria2_options: dict[str, str | int | float | bool] = Field(default_factory=dict)


class ChEMBLConnector(BaseConnector):
    """Connector for ingesting SMILES data from bulk ChEMBL SDF archives."""

    config: ChEMBLConfig

    def __init__(
        self,
        config: ChEMBLConfig,
        checkpoint_manager: CheckpointManager,
        aria2_downloader: Callable[..., None] | None = None,
    ) -> None:
        super().__init__(config=config, checkpoint_manager=checkpoint_manager)
        self._download_dir = self._resolve_download_dir()
        self._aria2_downloader = aria2_downloader or download_with_aria2
        self._aria2_options = self._build_aria2_options()
        self._entries = self._parse_link_file(config.link_file)

    def _resolve_download_dir(self) -> Path:
        if self.config.download_dir is not None:
            return self.config.download_dir
        return self.config.link_file.resolve().parent

    def _build_aria2_options(self) -> Aria2Options:
        options = self.config.aria2_options
        if not options:
            return Aria2Options()
        try:
            return Aria2Options(**options)
        except TypeError as exc:  # pragma: no cover - defensive guard
            raise ValueError("Invalid aria2 options supplied") from exc

    def _parse_link_file(self, path: Path) -> list[_ChEMBLEntry]:
        if not path.exists():
            raise FileNotFoundError(f"ChEMBL link file not found: {path}")

        entries: list[_ChEMBLEntry] = []
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            filename = Path(line).name
            if not filename:
                raise ValueError(f"Unable to determine filename for URL: {line}")
            entries.append(_ChEMBLEntry(filename=filename, url=line))

        if not entries:
            raise ValueError(f"No download URLs found in {path}")
        return entries

    def _ensure_archive(self, entry: _ChEMBLEntry) -> Path | None:
        target = self._download_dir / entry.filename
        if target.exists() and target.stat().st_size > 0:
            return target
        try:
            self._aria2_downloader(
                entry.url,
                target,
                options=self._aria2_options,
                skip_existing=True,
            )
        except subprocess.CalledProcessError as exc:
            logger.error(
                "ingestion.chembl.download_failed",
                source=self.config.name,
                url=entry.url,
                output=str(target),
                returncode=exc.returncode,
            )
            return None
        return target

    @property
    def download_directory(self) -> Path:
        """Return the directory used to cache downloaded archives."""

        return self._download_dir

    def download_archives(self) -> list[Path]:
        """Ensure all referenced archives are present locally."""

        downloaded: list[Path] = []
        for entry in self._entries:
            archive = self._ensure_archive(entry)
            if archive is None:
                logger.warning(
                    "ingestion.chembl.skip_entry",
                    source=self.config.name,
                    url=entry.url,
                )
                continue
            downloaded.append(archive)
        return downloaded

    def _resolve_local_archive(self, entry: _ChEMBLEntry) -> Path:
        target = self._download_dir / entry.filename
        if not target.exists() or target.stat().st_size == 0:
            raise FileNotFoundError(
                f"ChEMBL archive not found: {target}. Run 'smiles download' first."
            )
        return target

    def _build_record(self, properties: Mapping[str, str]) -> MoleculeRecord:
        identifier = properties.get(self.config.identifier_tag, "").strip()
        smiles = properties.get(self.config.smiles_tag, "").strip()
        metadata: dict[str, str] = {
            key: value
            for key, value in properties.items()
            if key not in {self.config.identifier_tag, self.config.smiles_tag}
        }
        if self.config.metadata_tags:
            metadata = {
                key: metadata[key]
                for key in self.config.metadata_tags
                if key in metadata
            }
        metadata = {key: value for key, value in metadata.items() if value}
        return MoleculeRecord(
            source=self.config.name,
            identifier=identifier,
            smiles=smiles,
            metadata=metadata,
        )

    def _iter_records(self, entry: _ChEMBLEntry) -> Iterator[MoleculeRecord]:
        archive = self._resolve_local_archive(entry)
        for properties in iter_sdf_records(archive):
            yield self._build_record(properties)

    def fetch_pages(self) -> Iterator[IngestionPage]:
        checkpoint = self._checkpoint_manager.load(self.config.name)
        if checkpoint and checkpoint.completed:
            logger.info("ingestion.skip", source=self.config.name, reason="completed")
            return

        start_file = 0
        start_offset = 0
        if checkpoint:
            start_file = int(checkpoint.cursor.get("file_index", 0))
            start_offset = int(checkpoint.cursor.get("record_offset", 0))

        batch: list[MoleculeRecord] = []
        entries = self._entries
        for file_index in range(start_file, len(entries)):
            entry = entries[file_index]
            record_offset = start_offset if file_index == start_file else 0
            processed = 0

            for record in self._iter_records(entry):
                if processed < record_offset:
                    processed += 1
                    continue
                batch.append(record)
                processed += 1
                if len(batch) >= self.config.batch_size:
                    next_cursor = {
                        "file_index": file_index,
                        "file_name": entry.filename,
                        "record_offset": processed,
                    }
                    yield IngestionPage(records=list(batch), next_cursor=next_cursor)
                    batch.clear()

            start_offset = 0

            if batch:
                next_cursor = (
                    {
                        "file_index": file_index + 1,
                        "file_name": entries[file_index + 1].filename
                        if file_index + 1 < len(entries)
                        else None,
                        "record_offset": 0,
                    }
                    if file_index + 1 < len(entries)
                    else None
                )
                yield IngestionPage(records=list(batch), next_cursor=next_cursor)
                batch.clear()

        if not entries:
            yield IngestionPage(records=[], next_cursor=None)

    def close(self) -> None:  # pragma: no cover - nothing to close
        return


__all__ = ["ChEMBLConfig", "ChEMBLConnector"]
