"""Ingestion orchestrator and configuration loader."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import structlog
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
from .pubchem import PubChemConfig, PubChemConnector
from .zinc import ZincConfig, ZincConnector

logger = structlog.get_logger(__name__)

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

    data = yaml.safe_load(path.read_text())
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
    if issubclass(connector_cls, BaseHttpConnector):
        if client_factory is not None:
            kwargs["client_factory"] = client_factory
    elif client_factory is not None:
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
) -> None:
    checkpoint_manager = CheckpointManager(job_config.checkpoint_dir / "ingestion")
    writer = NDJSONWriter(job_config.output_dir, compress=job_config.compress_output)
    connector = _build_connector(
        definition,
        checkpoint_manager,
        default_batch_size=job_config.batch_size,
        client_factory=client_factory,
    )
    checkpoint = checkpoint_manager.load(definition.name)
    start_batch = checkpoint.batch_index if checkpoint else 0

    try:
        for page in connector.fetch_pages():
            _persist_page(writer, checkpoint_manager, definition.name, start_batch, page)
            if page.records:
                start_batch += 1
    finally:
        connector.close()


def run_ingestion(
    config: IngestionJobConfig,
    *,
    client_factories: Mapping[str, ClientFactory] | None = None,
) -> None:
    """Execute the configured ingestion job."""

    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _factory_for(definition: SourceDefinition) -> ClientFactory | None:
        if not client_factories:
            return None
        return client_factories.get(definition.name) or client_factories.get(definition.type)

    with ThreadPoolExecutor(max_workers=config.concurrency) as executor:
        futures = {
            executor.submit(_run_source, source, config, _factory_for(source)): source.name
            for source in config.sources
        }
        for future in as_completed(futures):
            source_name = futures[future]
            try:
                future.result()
            except Exception as exc:  # pragma: no cover - surfaced in tests
                logger.error("ingestion.failed", source=source_name, error=str(exc))
                raise
            else:
                logger.info("ingestion.completed", source=source_name)


__all__ = [
    "IngestionJobConfig",
    "SourceDefinition",
    "load_config",
    "run_ingestion",
]
