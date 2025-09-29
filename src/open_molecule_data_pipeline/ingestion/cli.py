"""Command-line interface for ingestion workflows."""

from __future__ import annotations

from pathlib import Path

import click

from ..logging_utils import get_logger
from .runner import load_config, run_ingestion

logger = get_logger(__name__)


@click.command(name="download")
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False, readable=True),
    required=True,
    help="Path to the ingestion job YAML configuration.",
)
def download_command(config_path: Path) -> None:
    """Download raw archives referenced in *config_path*."""

    job_config = load_config(config_path)
    logger.info("download.start", sources=[source.name for source in job_config.sources])
    run_ingestion(job_config)
    logger.info("download.finish")


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


__all__ = ["download_command", "ingest_command"]
