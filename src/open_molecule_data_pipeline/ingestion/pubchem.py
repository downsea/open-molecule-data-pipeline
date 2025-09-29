"""PubChem ingestion connector implementation using local index manifests."""

from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterator, Mapping
from urllib.parse import urljoin

from pydantic import Field

from ..logging_utils import get_logger
from .aria2 import Aria2Options, download_with_aria2
from .common import (
    BaseConnector,
    CheckpointManager,
    IngestionPage,
    MoleculeRecord,
    SourceConfig,
)
from .sdf import iter_sdf_records

logger = get_logger(__name__)


class _IndexLinkParser(HTMLParser):
    """Collect all hyperlinks from an HTML index page."""

    def __init__(self) -> None:
        super().__init__()
        self._links: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = next((value for key, value in attrs if key.lower() == "href"), None)
        if href:
            self._links.add(href.strip())

    def links(self) -> list[str]:
        return sorted(self._links)


@dataclass(frozen=True)
class _PubChemEntry:
    filename: str
    url: str
    checksum_url: str | None
    checksum_algorithm: str | None


class PubChemConfig(SourceConfig):
    """Configuration for downloading PubChem SDF bundles via :command:`aria2c`."""

    index_file: Path = Field(description="Path to the saved PubChem HTML index page.")
    download_dir: Path | None = Field(
        default=None,
        description="Directory where PubChem archives and checksum files are stored.",
    )
    base_url: str | None = Field(
        default=None,
        description="Optional base URL used when links inside the index are relative.",
    )
    identifier_tag: str = "PUBCHEM_COMPOUND_CID"
    smiles_tag: str = "PUBCHEM_OPENEYE_ISO_SMILES"
    metadata_tags: list[str] = Field(default_factory=list)
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
        self._entries = self._parse_index(config.index_file)

    def _resolve_download_dir(self) -> Path:
        if self.config.download_dir is not None:
            return self.config.download_dir
        return self.config.index_file.resolve().parent

    def _build_aria2_options(self) -> Aria2Options:
        options = self.config.aria2_options
        if not options:
            return Aria2Options()
        try:
            return Aria2Options(**options)
        except TypeError as exc:  # pragma: no cover - validation guard
            raise ValueError("Invalid aria2 options supplied") from exc

    def _parse_index(self, path: Path) -> list[_PubChemEntry]:
        if not path.exists():
            raise FileNotFoundError(f"PubChem index not found: {path}")

        parser = _IndexLinkParser()
        parser.feed(path.read_text())
        parser.close()

        sdf_links: dict[str, str] = {}
        checksum_links: dict[str, tuple[str, str]] = {}

        for raw_href in parser.links():
            resolved = self._resolve_href(raw_href)
            name = Path(raw_href).name or Path(resolved).name
            if not name:
                continue
            if name.endswith(".md5"):
                target = name[:-4]
                checksum_links[target] = (resolved, "md5")
            elif name.endswith((".sdf", ".sdf.gz")):
                sdf_links[name] = resolved

        entries: list[_PubChemEntry] = []
        for filename in sorted(sdf_links):
            checksum_url: str | None = None
            checksum_alg: str | None = None
            if filename in checksum_links:
                checksum_url, checksum_alg = checksum_links[filename]
            entries.append(
                _PubChemEntry(
                    filename=filename,
                    url=sdf_links[filename],
                    checksum_url=checksum_url,
                    checksum_algorithm=checksum_alg,
                )
            )

        if not entries:
            raise ValueError(f"No SDF entries discovered in index: {path}")
        return entries

    def _resolve_href(self, href: str) -> str:
        if href.startswith("http://") or href.startswith("https://"):
            return href
        if self.config.base_url is None:
            raise ValueError(
                "Index contains relative links but no base_url was configured: "
                f"{href}"
            )
        return urljoin(self.config.base_url, href)

    def _checksum_path(self, entry: _PubChemEntry) -> Path:
        return self._download_dir / f"{entry.filename}.md5"

    def _load_checksum(self, entry: _PubChemEntry) -> tuple[str, str] | None:
        if not entry.checksum_url or not entry.checksum_algorithm:
            return None
        checksum_path = self._checksum_path(entry)
        if not checksum_path.exists() or checksum_path.stat().st_size == 0:
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

    def _ensure_archive(self, entry: _PubChemEntry) -> Path:
        target = self._download_dir / entry.filename
        checksum = self._load_checksum(entry)
        skip_existing = checksum is None
        kwargs: dict[str, object] = {"options": self._aria2_options, "skip_existing": skip_existing}
        if checksum:
            kwargs["checksum"] = checksum
        self._aria2_downloader(entry.url, target, **kwargs)
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

    def _iter_records(self, entry: _PubChemEntry) -> Iterator[MoleculeRecord]:
        archive = self._ensure_archive(entry)
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


__all__ = ["PubChemConfig", "PubChemConnector"]
