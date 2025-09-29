"""Command-line interface for ingestion workflows."""

from __future__ import annotations

from pathlib import Path

import click
import structlog

from .runner import load_config, run_ingestion

logger = structlog.get_logger(__name__)


@click.command(name="ingest")
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False, readable=True),
    required=True,
    help="Path to the ingestion job YAML configuration.",
)
def ingest_command(config_path: Path) -> None:
    """Run the SMILES ingestion job defined in *config_path*."""

    job_config = load_config(config_path)
    logger.info("ingestion.start", sources=[source.name for source in job_config.sources])
    run_ingestion(job_config)
    logger.info("ingestion.finish")


__all__ = ["ingest_command"]
