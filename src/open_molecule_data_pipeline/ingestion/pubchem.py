"""PubChem ingestion connector implementation using manifest link files."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pydantic import Field

from ..logging_utils import get_logger
from .aria2 import Aria2Options, download_with_aria2
from .common import BaseConnector, CheckpointManager, SourceConfig

logger = get_logger(__name__)


@dataclass(frozen=True)
class _PubChemEntry:
    filename: str
    url: str
    checksum_url: str | None
    checksum_algorithm: str | None


class PubChemConfig(SourceConfig):
    """Configuration for downloading PubChem SDF bundles via :command:`aria2c`."""

    link_file: Path = Field(
        description="Path to the newline-delimited list of PubChem SDF URLs."
    )

    download_dir: Path | None = Field(
        default=None,
        description="Directory where PubChem archives and checksum files are stored.",
    )
    checksum_suffix: str | None = Field(
        default=".md5",
        description=(
            "Optional suffix appended to each SDF URL to locate its checksum file. "
            "Set to null to skip checksum downloads."
        ),
    )
    checksum_algorithm: str | None = Field(
        default="md5",
        description="Checksum algorithm used when checksum files are downloaded.",
    )
    identifier_tag: str = Field(
        default="PUBCHEM_COMPOUND_CID",
        description="Identifier field retained for backwards compatibility.",
    )
    smiles_tag: str = Field(
        default="PUBCHEM_OPENEYE_ISO_SMILES",
        description="SMILES field retained for backwards compatibility.",
    )
    metadata_tags: list[str] = Field(
        default_factory=list,
        description="Metadata fields retained for backwards compatibility.",
    )
    aria2_options: dict[str, str | int | float | bool] = Field(default_factory=dict)


class PubChemConnector(BaseConnector):
    """Connector that downloads and parses PubChem SDF archives."""

    config: PubChemConfig

    def __init__(
        self,
        config: PubChemConfig,
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
        except TypeError as exc:  # pragma: no cover - validation guard
            raise ValueError("Invalid aria2 options supplied") from exc

    def _parse_link_file(self, path: Path) -> list[_PubChemEntry]:
        if not path.exists():
            raise FileNotFoundError(f"PubChem link file not found: {path}")

        entries: list[_PubChemEntry] = []
        checksum_suffix = self.config.checksum_suffix
        checksum_algorithm = self.config.checksum_algorithm if checksum_suffix else None

        for line_number, raw_line in enumerate(
            path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1
        ):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            url = line.split()[0]
            filename = Path(url).name
            if not filename:
                raise ValueError(
                    f"Unable to determine filename for PubChem URL on line {line_number}: {line}"
                )

            checksum_url: str | None = None
            checksum_alg: str | None = None
            if checksum_suffix and checksum_algorithm:
                checksum_url = f"{url}{checksum_suffix}"
                checksum_alg = checksum_algorithm

            entries.append(
                _PubChemEntry(
                    filename=filename,
                    url=url,
                    checksum_url=checksum_url,
                    checksum_algorithm=checksum_alg,
                )
            )

        if not entries:
            raise ValueError(f"No SDF URLs discovered in link file: {path}")
        return entries

    def _checksum_path(self, entry: _PubChemEntry) -> Path:
        suffix = self.config.checksum_suffix or ""
        return self._download_dir / f"{entry.filename}{suffix}"

    def _load_checksum(self, entry: _PubChemEntry) -> tuple[str, str] | None:
        if not entry.checksum_url or not entry.checksum_algorithm:
            return None
        checksum_path = self._checksum_path(entry)
        if not checksum_path.exists() or checksum_path.stat().st_size == 0:
            checksum_path.parent.mkdir(parents=True, exist_ok=True)
            self._aria2_downloader(
                entry.checksum_url,
                checksum_path,
                options=self._aria2_options,
                skip_existing=False,
            )
        content = checksum_path.read_text(encoding="utf-8", errors="ignore").strip()
        if not content:
            raise ValueError(f"Checksum file is empty: {checksum_path}")
        value = content.split()[0]
        return (entry.checksum_algorithm, value)

    def _ensure_archive(self, entry: _PubChemEntry) -> Path | None:
        target = self._download_dir / entry.filename
        target.parent.mkdir(parents=True, exist_ok=True)
        checksum = self._load_checksum(entry)
        skip_existing = checksum is None
        kwargs: dict[str, object] = {"options": self._aria2_options, "skip_existing": skip_existing}
        if checksum:
            kwargs["checksum"] = checksum
        try:
            self._aria2_downloader(entry.url, target, **kwargs)
        except subprocess.CalledProcessError as exc:
            logger.error(
                "ingestion.pubchem.download_failed",
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
        """Ensure all referenced archives and checksums are present locally."""

        downloaded: list[Path] = []
        for entry in self._entries:
            archive = self._ensure_archive(entry)
            if archive is None:
                logger.warning(
                    "ingestion.pubchem.skip_entry",
                    source=self.config.name,
                    url=entry.url,
                )
                continue
            downloaded.append(archive)
        return downloaded

    def close(self) -> None:  # pragma: no cover - nothing to close
        return


__all__ = ["PubChemConfig", "PubChemConnector"]
