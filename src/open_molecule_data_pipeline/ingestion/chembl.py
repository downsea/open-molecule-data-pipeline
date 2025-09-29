"""ChEMBL ingestion connector using bulk SDF downloads."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pydantic import Field

from .aria2 import Aria2Options, download_with_aria2
from .common import BaseConnector, CheckpointManager, SourceConfig
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

    def close(self) -> None:  # pragma: no cover - nothing to close
        return


__all__ = ["ChEMBLConfig", "ChEMBLConnector"]
