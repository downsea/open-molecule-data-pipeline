"""ZINC database ingestion via tranche URI manifests."""

from __future__ import annotations

import gzip
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Callable, Iterator
from urllib.parse import urlparse

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

logger = get_logger(__name__)


@dataclass(frozen=True)
class _TrancheResource:
    """Description of a single tranche archive referenced in the manifest."""

    url: str
    relative_path: Path


class ZincConfig(SourceConfig):
    """Configuration for ingesting ZINC tranche downloads."""

    uri_file: Path = Field(
        description="Path to the newline-delimited list of tranche archive URLs."
    )
    download_dir: Path | None = Field(
        default=None,
        description="Directory containing downloaded tranche archives.",
    )
    download_missing: bool = Field(
        default=False,
        description=(
            "If True, missing tranche archives will be downloaded automatically "
            "using the URLs in the wget script."
        ),
    )
    aria2_options: dict[str, str | int | float | bool] = Field(
        default_factory=dict,
        description="Optional overrides for aria2c concurrency and retry flags.",
    )
    username: str | None = Field(
        default=None,
        description="Optional username for authenticated downloads.",
    )
    password: str | None = Field(
        default=None,
        description="Optional password for authenticated downloads.",
    )
    delimiter: str | None = Field(
        default="\t",
        description="Column delimiter in decompressed SMILES files.",
    )
    smiles_column: int = Field(
        default=0,
        ge=0,
        description="Index of the SMILES column in tranche files.",
    )
    identifier_column: int = Field(
        default=1,
        ge=0,
        description="Index of the identifier column in tranche files.",
    )


class ZincConnector(BaseConnector):
    """Connector that streams SMILES from ZINC tranche downloads."""

    config: ZincConfig

    def __init__(
        self,
        config: ZincConfig,
        checkpoint_manager: CheckpointManager,
        aria2_downloader: Callable[..., None] | None = None,
    ) -> None:
        super().__init__(config=config, checkpoint_manager=checkpoint_manager)
        self._download_dir = self._resolve_download_dir()
        self._resources = self._parse_uri_file(config.uri_file)
        self._aria2_downloader = aria2_downloader or download_with_aria2
        self._aria2_options = self._build_aria2_options()

    def _build_aria2_options(self) -> Aria2Options:
        if not self.config.aria2_options:
            return Aria2Options()
        try:
            return Aria2Options(**self.config.aria2_options)
        except TypeError as exc:  # pragma: no cover - defensive guard
            raise ValueError("Invalid aria2 configuration supplied") from exc

    def _resolve_download_dir(self) -> Path:
        if self.config.download_dir is not None:
            return self.config.download_dir
        return self.config.uri_file.resolve().parent

    def _parse_uri_file(self, path: Path) -> list[_TrancheResource]:
        if not path.exists():
            raise FileNotFoundError(f"URI manifest not found: {path}")

        resources: list[_TrancheResource] = []
        for line_number, raw_line in enumerate(
            path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1
        ):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            url = line.split()[0]
            parsed = urlparse(url)
            if not parsed.path:
                raise ValueError(
                    f"Unable to determine output path for URL on line {line_number}: {line}"
                )
            relative = PurePosixPath(parsed.path.lstrip("/"))
            if not relative.parts:
                raise ValueError(
                    f"Invalid URL with empty path on line {line_number}: {line}"
                )
            resources.append(_TrancheResource(url=url, relative_path=Path(*relative.parts)))

        if not resources:
            raise ValueError(f"No tranche URLs found in manifest: {path}")
        return resources

    def _ensure_archive(self, resource: _TrancheResource) -> Path | None:
        target_path = self._download_dir / resource.relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if target_path.exists() and target_path.stat().st_size > 0:
            return target_path

        if not self.config.download_missing:
            raise FileNotFoundError(
                f"Required tranche archive is missing: {target_path}. "
                "Set download_missing=True to fetch automatically."
            )

        logger.info(
            "ingestion.zinc.download",
            source=self.config.name,
            url=resource.url,
            output=str(target_path),
        )
        kwargs: dict[str, object] = {"options": self._aria2_options, "skip_existing": False}
        if self.config.username:
            kwargs["username"] = self.config.username
        if self.config.password:
            kwargs["password"] = self.config.password
        try:
            self._aria2_downloader(resource.url, target_path, **kwargs)
        except subprocess.CalledProcessError as exc:
            logger.error(
                "ingestion.zinc.download_failed",
                source=self.config.name,
                url=resource.url,
                output=str(target_path),
                returncode=exc.returncode,
            )
            return None
        return target_path

    def _iter_records(self, resource: _TrancheResource) -> Iterator[MoleculeRecord]:
        archive_path = self._ensure_archive(resource)
        if archive_path is None:
            logger.warning(
                "ingestion.zinc.skip_entry",
                source=self.config.name,
                file=resource.relative_path.as_posix(),
                url=resource.url,
            )
            return
        delimiter = self.config.delimiter
        smiles_index = self.config.smiles_column
        identifier_index = self.config.identifier_column

        if archive_path.suffix == ".gz":
            stream_ctx = gzip.open(archive_path, "rt", encoding="utf-8", errors="replace")
        else:
            stream_ctx = archive_path.open("r", encoding="utf-8", errors="replace")

        with stream_ctx as stream:
            for line_number, raw_line in enumerate(stream, start=1):
                line = raw_line.strip()
                if not line:
                    continue
                parts = line.split(delimiter) if delimiter else line.split()
                if (
                    len(parts) <= max(smiles_index, identifier_index)
                    or not parts[smiles_index]
                    or not parts[identifier_index]
                ):
                    logger.debug(
                        "ingestion.zinc.skip_line",
                        source=self.config.name,
                        file=str(resource.relative_path),
                        line=line_number,
                    )
                    continue

                smiles = parts[smiles_index].strip()
                identifier = parts[identifier_index].strip()
                metadata = {
                    "source_file": resource.relative_path.as_posix(),
                    "download_url": resource.url,
                }
                for idx, value in enumerate(parts):
                    if idx in {smiles_index, identifier_index}:
                        continue
                    key = f"column_{idx}"
                    metadata[key] = value.strip()

                yield MoleculeRecord(
                    source=self.config.name,
                    identifier=identifier,
                    smiles=smiles,
                    metadata=metadata,
                )

    @property
    def download_directory(self) -> Path:
        """Return the directory used to cache tranche archives."""

        return self._download_dir

    def fetch_pages(self) -> Iterator[IngestionPage]:
        checkpoint = self._checkpoint_manager.load(self.config.name)
        if checkpoint and checkpoint.completed:
            logger.info("ingestion.skip", source=self.config.name, reason="completed")
            return

        start_entry = 0
        start_offset = 0
        if checkpoint:
            start_entry = int(checkpoint.cursor.get("entry_index", 0))
            start_offset = int(checkpoint.cursor.get("line_offset", 0))

        batch: list[MoleculeRecord] = []
        resources = self._resources
        for resource_index in range(start_entry, len(resources)):
            resource = resources[resource_index]
            line_offset = start_offset if resource_index == start_entry else 0
            processed_lines = 0

            for record in self._iter_records(resource):
                if processed_lines < line_offset:
                    processed_lines += 1
                    continue

                batch.append(record)
                processed_lines += 1
                if len(batch) >= self.config.batch_size:
                    next_cursor = {
                        "entry_index": resource_index,
                        "line_offset": processed_lines,
                        "file": resource.relative_path.as_posix(),
                    }
                    yield IngestionPage(records=list(batch), next_cursor=next_cursor)
                    batch.clear()

            if line_offset and processed_lines < line_offset:
                logger.warning(
                    "ingestion.zinc.offset_exceeds_file",
                    source=self.config.name,
                    file=resource.relative_path.as_posix(),
                    expected_offset=line_offset,
                    available_records=processed_lines,
                )
                line_offset = processed_lines

            start_offset = 0

            next_cursor: dict[str, int] | None
            if batch:
                if resource_index + 1 < len(resources):
                    next_cursor = {"entry_index": resource_index + 1, "line_offset": 0}
                else:
                    next_cursor = None
                yield IngestionPage(records=list(batch), next_cursor=next_cursor)
                batch.clear()
            elif processed_lines == 0:
                if resource_index + 1 < len(resources):
                    next_cursor = {"entry_index": resource_index + 1, "line_offset": 0}
                else:
                    next_cursor = None
                yield IngestionPage(records=[], next_cursor=next_cursor)

        if not resources:
            yield IngestionPage(records=[], next_cursor=None)

__all__ = ["ZincConfig", "ZincConnector"]

