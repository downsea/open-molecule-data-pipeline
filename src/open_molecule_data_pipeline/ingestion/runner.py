"""Ingestion orchestrator and configuration loader."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import inspect

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from .chembl import ChEMBLConfig, ChEMBLConnector
from .chemspider import ChemSpiderConfig, ChemSpiderConnector
from .common import (
    BaseConnector,
    BaseHttpConnector,
    CheckpointManager,
    IngestionCheckpoint,
    IngestionPage,
    NDJSONWriter,
    SourceConfig,
)
from ..logging_utils import get_logger
from .pubchem import PubChemConfig, PubChemConnector
from .zinc import ZincConfig, ZincConnector

logger = get_logger(__name__)

ClientFactory = Callable[..., object]


@dataclass(frozen=True)
class ConnectorRegistration:
    """Associates a connector implementation with its config model."""

    connector: type[BaseConnector]
    config: type[SourceConfig]


CONNECTOR_REGISTRY: dict[str, ConnectorRegistration] = {
    "pubchem": ConnectorRegistration(PubChemConnector, PubChemConfig),
    "zinc": ConnectorRegistration(ZincConnector, ZincConfig),
    "chembl": ConnectorRegistration(ChEMBLConnector, ChEMBLConfig),
    "chemspider": ConnectorRegistration(ChemSpiderConnector, ChemSpiderConfig),
}


@dataclass(frozen=True)
class DirectorySummary:
    """Aggregated statistics for files contained in a directory."""

    directory: Path
    file_count: int
    total_bytes: int


@dataclass(frozen=True)
class SourceIngestionSummary:
    """Execution summary for a single ingestion source."""

    name: str
    type: str
    completed: bool
    total_batches: int
    batches_written: int
    records_written: int
    output: DirectorySummary
    downloads: DirectorySummary | None


class SourceDefinition(BaseModel):
    """Declarative representation of a single ingestion source."""

    type: str
    name: str
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_known_type(self) -> SourceDefinition:
        if self.type not in CONNECTOR_REGISTRY:
            msg = ", ".join(sorted(CONNECTOR_REGISTRY))
            raise ValueError(f"Unknown source type '{self.type}'. Available types: {msg}")
        return self


class IngestionJobConfig(BaseModel):
    """Top-level configuration for an ingestion job."""

    output_dir: Path
    checkpoint_dir: Path
    batch_size: int = Field(default=1000, gt=0)
    concurrency: int = Field(default=1, gt=0)
    compress_output: bool = True
    sources: list[SourceDefinition]

    @model_validator(mode="after")
    def ensure_unique_names(self) -> IngestionJobConfig:
        names = {source.name for source in self.sources}
        if len(names) != len(self.sources):
            raise ValueError("Source names must be unique")
        return self


class IngestionJob(BaseModel):
    """Wrapper for loading YAML files containing job definitions."""

    job: IngestionJobConfig


def load_config(path: Path) -> IngestionJobConfig:
    """Load an :class:`IngestionJobConfig` from a YAML file."""

    data = yaml.safe_load(path.read_text(encoding="utf-8", errors="replace"))
    try:
        wrapper = IngestionJob.model_validate(data)
    except ValidationError as exc:  # pragma: no cover - pydantic already tested
        raise ValueError(f"Invalid ingestion config: {exc}") from exc
    return wrapper.job


def _build_connector(
    definition: SourceDefinition,
    checkpoint_manager: CheckpointManager,
    default_batch_size: int,
    client_factory: ClientFactory | None = None,
) -> BaseConnector:
    registration = CONNECTOR_REGISTRY[definition.type]
    config_kwargs = dict(definition.options)
    config_kwargs.setdefault("batch_size", default_batch_size)
    config = registration.config(name=definition.name, **config_kwargs)
    connector_cls = registration.connector
    kwargs: dict[str, object] = {
        "config": config,
        "checkpoint_manager": checkpoint_manager,
    }
    if client_factory is not None:
        signature = inspect.signature(connector_cls.__init__)
        if "client_factory" in signature.parameters:
            kwargs["client_factory"] = client_factory
        elif "ftp_factory" in signature.parameters:
            kwargs["ftp_factory"] = client_factory
    return connector_cls(**kwargs)  # type: ignore[arg-type]


def _persist_page(
    writer: NDJSONWriter,
    checkpoint_manager: CheckpointManager,
    source_name: str,
    start_batch: int,
    page: IngestionPage,
) -> None:
    if not page.records:
        checkpoint_manager.store(
            source_name,
            IngestionCheckpoint(
                cursor=page.next_cursor or {},
                batch_index=start_batch,
                completed=page.next_cursor is None,
            ),
        )
        return

    batch_index = start_batch + 1
    writer.write_batch(source_name, batch_index, page.records)
    checkpoint_manager.store(
        source_name,
        IngestionCheckpoint(
            cursor=page.next_cursor or {},
            batch_index=batch_index,
            completed=page.next_cursor is None,
        ),
    )


def _run_source(
    definition: SourceDefinition,
    job_config: IngestionJobConfig,
    client_factory: ClientFactory | None,
    checkpoint_root: Path,
    mode: str,
) -> SourceIngestionSummary:
    checkpoint_manager = CheckpointManager(checkpoint_root)
    writer = NDJSONWriter(job_config.output_dir, compress=job_config.compress_output)
    connector = _build_connector(
        definition,
        checkpoint_manager,
        default_batch_size=job_config.batch_size,
        client_factory=client_factory,
    )
    checkpoint = checkpoint_manager.load(definition.name)
    start_batch = checkpoint.batch_index if checkpoint else 0
    batches_written = 0
    records_written = 0
    download_method = getattr(connector, "download_archives", None)

    try:
        if mode == "download" and callable(download_method):
            if checkpoint and checkpoint.completed:
                logger.info("download.skip", source=definition.name, reason="completed")
            else:
                downloaded = download_method()
                logger.info(
                    "download.complete",
                    source=definition.name,
                    files=len(downloaded),
                )
                checkpoint_manager.store(
                    definition.name,
                    IngestionCheckpoint(cursor={}, batch_index=0, completed=True),
                )
        else:
            if mode == "download" and not callable(download_method):
                logger.warning(
                    "download.unsupported",
                    source=definition.name,
                    reason="connector does not expose download_archives",
                )
            for page in connector.fetch_pages():
                _persist_page(writer, checkpoint_manager, definition.name, start_batch, page)
                if page.records:
                    start_batch += 1
                    batches_written += 1
                    records_written += len(page.records)
    finally:
        connector.close()

    final_checkpoint = checkpoint_manager.load(definition.name)
    if final_checkpoint:
        total_batches = final_checkpoint.batch_index
        completed = bool(final_checkpoint.completed)
    else:
        total_batches = start_batch
        completed = mode == "download"

    output_summary = _summarise_output_directory(job_config.output_dir / definition.name)
    download_summary = _summarise_downloads(connector)

    return SourceIngestionSummary(
        name=definition.name,
        type=definition.type,
        completed=completed,
        total_batches=total_batches,
        batches_written=batches_written,
        records_written=records_written,
        output=output_summary,
        downloads=download_summary,
    )


def _normalise_path(path: Path) -> Path:
    """Return an absolute version of *path* without requiring it to exist."""

    try:
        return path.resolve()
    except OSError:  # pragma: no cover - filesystem-dependent edge cases
        return path


def _iter_files(directory: Path, patterns: Sequence[str] | None) -> list[Path]:
    """Return the list of files matching *patterns* within *directory*."""

    if not directory.exists():
        return []
    if not patterns:
        return [path for path in directory.rglob("*") if path.is_file()]
    matches: list[Path] = []
    for pattern in patterns:
        matches.extend(path for path in directory.glob(pattern) if path.is_file())
    return matches


def _summarise_directory(path: Path, patterns: Sequence[str] | None) -> DirectorySummary:
    """Compute file statistics for *path* based on optional *patterns*."""

    directory = _normalise_path(path)
    files = _iter_files(directory, patterns)
    total_bytes = sum(file.stat().st_size for file in files)
    return DirectorySummary(directory=directory, file_count=len(files), total_bytes=total_bytes)


def _summarise_output_directory(path: Path) -> DirectorySummary:
    """Summarise the generated NDJSON batches for a source."""

    return _summarise_directory(path, patterns=("*.jsonl", "*.jsonl.gz"))


def _summarise_downloads(connector: BaseConnector) -> DirectorySummary | None:
    """Summarise cached download artifacts exposed by *connector*."""

    download_dir = getattr(connector, "download_directory", None)
    if download_dir is None:
        return None
    return _summarise_directory(Path(download_dir), patterns=None)


def _format_bytes(size: int) -> str:
    """Render *size* in bytes using a human-readable unit."""

    if size <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{int(value)} B"  # pragma: no cover - fallback


def _write_raw_data_report(
    config: IngestionJobConfig, summaries: Sequence[SourceIngestionSummary]
) -> None:
    """Persist a Markdown report summarising raw data downloads."""

    report_dir = _normalise_path(config.output_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "raw-data-report.md"

    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    lines: list[str] = [
        "# Raw Data Download Report",
        "",
        f"Generated: {timestamp}",
        "",
    ]

    if summaries:
        lines.append(
            "| Source | Type | Completed | Total Batches | Batches (run) | Records (run) | "
            "Output Files | Output Size | Download Files | Download Size |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for summary in summaries:
            download_files = (
                f"{summary.downloads.file_count:,}"
                if summary.downloads is not None
                else "n/a"
            )
            download_size = (
                _format_bytes(summary.downloads.total_bytes)
                if summary.downloads is not None
                else "n/a"
            )
            lines.append(
                "| "
                f"{summary.name} | {summary.type} | {'yes' if summary.completed else 'no'} | "
                f"{summary.total_batches:,} | {summary.batches_written:,} | {summary.records_written:,} | "
                f"{summary.output.file_count:,} | {_format_bytes(summary.output.total_bytes)} | "
                f"{download_files} | {download_size} |"
            )
    else:
        lines.append("No sources were executed.")

    for summary in summaries:
        lines.extend([
            "",
            f"## {summary.name}",
            "",
            f"- **Source type**: {summary.type}",
            f"- **Completed**: {'yes' if summary.completed else 'no'}",
            f"- **Total batches**: {summary.total_batches:,}",
            f"- **Batches written this run**: {summary.batches_written:,}",
            f"- **Records written this run**: {summary.records_written:,}",
            f"- **Output directory**: `{summary.output.directory.as_posix()}`",
            f"- **Output artifacts**: {summary.output.file_count:,} files totaling {_format_bytes(summary.output.total_bytes)}",
        ])

        if summary.downloads is not None:
            lines.append(
                f"- **Download cache**: `{summary.downloads.directory.as_posix()}` "
                f"({summary.downloads.file_count:,} files, "
                f"{_format_bytes(summary.downloads.total_bytes)})"
            )
        else:
            lines.append("- **Download cache**: n/a")

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_ingestion(
    config: IngestionJobConfig,
    *,
    client_factories: Mapping[str, ClientFactory] | None = None,
    mode: str = "download",
) -> None:
    """Execute the configured ingestion job."""

    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _factory_for(definition: SourceDefinition) -> ClientFactory | None:
        if not client_factories:
            return None
        return client_factories.get(definition.name) or client_factories.get(definition.type)

    summaries: list[SourceIngestionSummary] = []
    checkpoint_root = config.checkpoint_dir / (
        "ingestion-download" if mode == "download" else "ingestion-parse"
    )

    with ThreadPoolExecutor(max_workers=config.concurrency) as executor:
        futures = {
            executor.submit(
                _run_source,
                source,
                config,
                _factory_for(source),
                checkpoint_root,
                mode,
            ): source.name
            for source in config.sources
        }
        for future in as_completed(futures):
            source_name = futures[future]
            try:
                summary = future.result()
            except Exception as exc:  # pragma: no cover - surfaced in tests
                logger.error("ingestion.failed", source=source_name, error=str(exc))
                raise
            else:
                logger.info("ingestion.completed", source=source_name)
                summaries.append(summary)

    summaries.sort(key=lambda item: item.name)
    _write_raw_data_report(config, summaries)


__all__ = [
    "IngestionJobConfig",
    "SourceDefinition",
    "load_config",
    "run_ingestion",
]
