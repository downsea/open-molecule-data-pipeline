"""PubChem ingestion connector implementation."""

from __future__ import annotations

import contextlib
import ftplib
import gzip
from collections.abc import Callable, Iterable, Iterator, Mapping
from pathlib import Path
import tempfile
from typing import Any

import structlog
from pydantic import Field

from .common import BaseConnector, CheckpointManager, IngestionPage, MoleculeRecord, SourceConfig

logger = structlog.get_logger(__name__)


class PubChemConfig(SourceConfig):
    """Configuration for downloading PubChem SDF bundles over FTP."""

    ftp_host: str = "ftp.ncbi.nlm.nih.gov"
    ftp_port: int = 21
    ftp_directory: str = "pubchem/Compound/CURRENT-Full/SDF"
    ftp_username: str = "anonymous"
    ftp_password: str = "anonymous@"
    passive_mode: bool = True
    timeout: float = 60.0
    file_suffixes: list[str] = Field(default_factory=lambda: [".sdf.gz", ".sdf"])
    identifier_tag: str = "PUBCHEM_COMPOUND_CID"
    smiles_tag: str = "PUBCHEM_OPENEYE_ISO_SMILES"
    metadata_tags: list[str] = Field(default_factory=list)


class PubChemConnector(BaseConnector):
    """Connector that downloads and parses PubChem SDF archives via FTP."""

    config: PubChemConfig

    def __init__(
        self,
        config: PubChemConfig,
        checkpoint_manager: CheckpointManager,
        ftp_factory: Callable[[], ftplib.FTP] | None = None,
    ) -> None:
        super().__init__(config=config, checkpoint_manager=checkpoint_manager)
        self._ftp_factory = ftp_factory or self._create_default_ftp
        self._ftp: ftplib.FTP | None = None

    def _create_default_ftp(self) -> ftplib.FTP:
        ftp = ftplib.FTP()
        ftp.connect(self.config.ftp_host, self.config.ftp_port, timeout=self.config.timeout)
        ftp.login(self.config.ftp_username, self.config.ftp_password)
        ftp.set_pasv(self.config.passive_mode)
        ftp.cwd(self.config.ftp_directory)
        return ftp

    def _ensure_ftp(self) -> ftplib.FTP:
        if self._ftp is None:
            ftp = self._ftp_factory()
            try:
                ftp.cwd(self.config.ftp_directory)
            except ftplib.all_errors:
                # Directory might already be set by the factory; ignore errors to avoid
                # breaking custom factories in tests.
                pass
            self._ftp = ftp
        return self._ftp

    def _list_remote_files(self, ftp: ftplib.FTP) -> list[str]:
        names = ftp.nlst()
        suffixes = tuple(self.config.file_suffixes)
        if suffixes:
            names = [name for name in names if name.endswith(suffixes)]
        return sorted(names)

    @contextlib.contextmanager
    def _download_file(self, ftp: ftplib.FTP, filename: str) -> Iterator[Path]:
        suffix = Path(filename).suffix
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
            ftp.retrbinary(f"RETR {filename}", handle.write)
            temp_path = Path(handle.name)
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

    def _iter_sdf_entries(self, ftp: ftplib.FTP, filename: str) -> Iterator[dict[str, str]]:
        with self._download_file(ftp, filename) as temp_path:
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

        ftp = self._ensure_ftp()
        filenames = self._list_remote_files(ftp)

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
            for entry in self._iter_sdf_entries(ftp, filename):
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
        if self._ftp is None:
            return
        with contextlib.suppress(Exception):
            self._ftp.quit()
        self._ftp = None


__all__ = ["PubChemConfig", "PubChemConnector"]
