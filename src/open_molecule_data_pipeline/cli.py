"""Top-level command group for the SMILES processing toolkit."""

from __future__ import annotations

import click

from .ingestion.cli import ingest_command


@click.group()
def main() -> None:
    """SMILES data pipeline command group."""


main.add_command(ingest_command)


__all__ = ["main"]
