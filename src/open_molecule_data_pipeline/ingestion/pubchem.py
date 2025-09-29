"""PubChem ingestion connector implementation."""

from __future__ import annotations

import contextlib
import gzip
import tempfile
from collections.abc import Callable, Iterable, Iterator, Mapping
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx
import structlog
from pydantic import Field

from .common import (
    BaseConnector,
    CheckpointManager,
    IngestionPage,
    MoleculeRecord,
    SourceConfig,
    DEFAULT_USER_AGENT,
    execute_request,
)

logger = structlog.get_logger(__name__)


class _DirectoryListingParser(HTMLParser):
    """Extract file names from an HTML directory index."""

    def __init__(self, suffixes: Iterable[str]) -> None:
        super().__init__()
        self._suffixes = tuple(suffixes)
        self._filenames: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str]]) -> None:
        if tag.lower() != "a":
            return
        href = None
        for key, value in attrs:
            if key.lower() == "href":
                href = value
                break
        if not href:
            return
        candidate = href.split("#", 1)[0].split("?", 1)[0].strip()
        if not candidate or not candidate.endswith(self._suffixes):
            return
        # Directory listings sometimes include relative paths; keep the base name.
        filename = candidate.rsplit("/", 1)[-1]
        if filename:
            self._filenames.add(filename)

    def get_filenames(self) -> list[str]:
        return sorted(self._filenames)


class PubChemConfig(SourceConfig):
    """Configuration for downloading PubChem SDF bundles over HTTPS."""

    base_url: str = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/"
    timeout: float = 60.0
    file_suffixes: list[str] = Field(default_factory=lambda: [".sdf.gz", ".sdf"])
    identifier_tag: str = "PUBCHEM_COMPOUND_CID"
    smiles_tag: str = "PUBCHEM_OPENEYE_ISO_SMILES"
    metadata_tags: list[str] = Field(default_factory=list)


class PubChemConnector(BaseConnector):
    """Connector that downloads and parses PubChem SDF archives via HTTPS."""

    config: PubChemConfig

    def __init__(
        self,
        config: PubChemConfig,
        checkpoint_manager: CheckpointManager,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        super().__init__(config=config, checkpoint_manager=checkpoint_manager)
        self._client_factory = client_factory or self._create_default_client
        self._client: httpx.Client | None = None

    def _create_default_client(self) -> httpx.Client:
        return httpx.Client(
            timeout=self.config.timeout,
            headers={"User-Agent": DEFAULT_USER_AGENT},
            follow_redirects=True,
        )

    def _ensure_client(self) -> httpx.Client:
        if self._client is None:
            self._client = self._client_factory()
        return self._client

    def _list_remote_files(self, client: httpx.Client) -> list[str]:
        request = client.build_request("GET", self.config.base_url)
        response = execute_request(client, request)
        try:
            parser = _DirectoryListingParser(self.config.file_suffixes)
            parser.feed(response.text)
            parser.close()
            return parser.get_filenames()
        finally:
            response.close()

    @contextlib.contextmanager
    def _download_file(self, client: httpx.Client, filename: str) -> Iterator[Path]:
        url = urljoin(self.config.base_url, filename)
        request = client.build_request("GET", url)
        response = execute_request(client, request)
        temp_path: Path | None = None
        try:
            suffix = Path(filename).suffix
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
                temp_path = Path(handle.name)
                for chunk in response.iter_bytes():
                    handle.write(chunk)
        finally:
            response.close()

        assert temp_path is not None  # pragma: no cover - defensive guard
        try:
            yield temp_path
        finally:
            with contextlib.suppress(FileNotFoundError):
                temp_path.unlink()

    @contextlib.contextmanager
    def _open_sdf_stream(self, path: Path) -> Iterator[Iterable[str]]:
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as stream:
                yield stream
        else:
            with path.open("r", encoding="utf-8", errors="replace") as stream:
                yield stream

    @staticmethod
    def _parse_entry(lines: list[str]) -> dict[str, str]:
        properties: dict[str, str] = {}
        current_tag: str | None = None
        buffer: list[str] = []
        for line in lines:
            if line.startswith(">"):
                if current_tag is not None:
                    properties[current_tag] = "\n".join(buffer).strip()
                start = line.find("<")
                end = line.find(">", start + 1)
                if start != -1 and end != -1:
                    current_tag = line[start + 1 : end].strip()
                    buffer = []
                else:
                    current_tag = None
                    buffer = []
            elif current_tag is not None:
                buffer.append(line.rstrip("\r"))
        if current_tag is not None:
            properties[current_tag] = "\n".join(buffer).strip()
        return properties

    def _iter_sdf_entries(self, client: httpx.Client, filename: str) -> Iterator[dict[str, str]]:
        with self._download_file(client, filename) as temp_path:
            with self._open_sdf_stream(temp_path) as stream:
                entry_lines: list[str] = []
                for raw_line in stream:
                    line = raw_line.rstrip("\n")
                    if line.strip() == "$$$$":
                        if entry_lines:
                            yield self._parse_entry(entry_lines)
                            entry_lines = []
                    else:
                        entry_lines.append(line)
                if entry_lines:
                    yield self._parse_entry(entry_lines)

    def _build_record(self, properties: Mapping[str, str]) -> MoleculeRecord:
        identifier = properties.get(self.config.identifier_tag, "").strip()
        smiles = properties.get(self.config.smiles_tag, "").strip()
        metadata: dict[str, Any] = {
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

    def fetch_pages(self) -> Iterator[IngestionPage]:
        checkpoint = self._checkpoint_manager.load(self.config.name)
        if checkpoint and checkpoint.completed:
            logger.info("ingestion.skip", source=self.config.name, reason="completed")
            return

        client = self._ensure_client()
        filenames = self._list_remote_files(client)

        start_file = 0
        start_offset = 0
        if checkpoint:
            start_file = int(checkpoint.cursor.get("file_index", 0))
            start_offset = int(checkpoint.cursor.get("record_offset", 0))

        if start_file >= len(filenames):
            yield IngestionPage(records=[], next_cursor=None)
            return

        batch: list[MoleculeRecord] = []
        current_file = start_file
        current_offset = start_offset
        yielded = False

        while current_file < len(filenames):
            filename = filenames[current_file]
            logger.info("ingestion.pubchem.file", source=self.config.name, file=filename)
            entry_index = 0
            for entry in self._iter_sdf_entries(client, filename):
                if entry_index < current_offset:
                    entry_index += 1
                    continue
                record = self._build_record(entry)
                batch.append(record)
                entry_index += 1
                if len(batch) >= self.config.batch_size:
                    next_cursor = {
                        "file_index": current_file,
                        "file_name": filename,
                        "record_offset": entry_index,
                    }
                    yielded = True
                    yield IngestionPage(records=list(batch), next_cursor=next_cursor)
                    batch.clear()
            current_file += 1
            current_offset = 0
            if batch:
                next_cursor = (
                    {
                        "file_index": current_file,
                        "file_name": filenames[current_file]
                        if current_file < len(filenames)
                        else None,
                        "record_offset": 0,
                    }
                    if current_file < len(filenames)
                    else None
                )
                yielded = True
                yield IngestionPage(records=list(batch), next_cursor=next_cursor)
                batch.clear()

        if not yielded:
            yield IngestionPage(records=[], next_cursor=None)

    def close(self) -> None:
        if self._client is None:
            return
        with contextlib.suppress(Exception):
            self._client.close()
        self._client = None


__all__ = ["PubChemConfig", "PubChemConnector"]
