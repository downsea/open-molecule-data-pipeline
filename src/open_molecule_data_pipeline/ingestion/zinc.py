"""ZINC database ingestion via tranche download scripts."""

from __future__ import annotations

import gzip
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import httpx
import structlog
from pydantic import Field

from .common import (
    BaseConnector,
    CheckpointManager,
    IngestionPage,
    MoleculeRecord,
    SourceConfig,
)

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class _WgetCommand:
    """Parsed representation of a single wget download command."""

    url: str
    output_path: Path
    username: str | None
    password: str | None


class ZincConfig(SourceConfig):
    """Configuration for ingesting ZINC tranche downloads."""

    wget_file: Path = Field(description="Path to the generated wget script." )
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
    ) -> None:
        super().__init__(config=config, checkpoint_manager=checkpoint_manager)
        self._download_dir = self._resolve_download_dir()
        self._commands = self._parse_wget_file(config.wget_file)

    def _resolve_download_dir(self) -> Path:
        if self.config.download_dir is not None:
            return self.config.download_dir
        return self.config.wget_file.resolve().parent

    def _parse_wget_file(self, path: Path) -> list[_WgetCommand]:
        if not path.exists():
            raise FileNotFoundError(f"wget script not found: {path}")

        commands: list[_WgetCommand] = []
        for line_number, raw_line in enumerate(path.read_text().splitlines(), start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            for segment in line.split("&&"):
                command = segment.strip()
                if not command:
                    continue
                tokens = shlex.split(command)
                if not tokens or tokens[0] != "wget":
                    continue
                parsed = self._parse_wget_tokens(tokens, line_number)
                commands.append(parsed)

        if not commands:
            raise ValueError(f"No wget commands found in script: {path}")
        return commands

    def _parse_wget_tokens(self, tokens: list[str], line_number: int) -> _WgetCommand:
        url: str | None = None
        output: Path | None = None
        username = self.config.username
        password = self.config.password

        index = 1
        while index < len(tokens):
            token = tokens[index]
            if token == "--user":
                index += 1
                if index < len(tokens):
                    username = tokens[index]
                index += 1
                continue
            if token == "--password":
                index += 1
                if index < len(tokens):
                    password = tokens[index]
                index += 1
                continue
            if token in {"-O", "--output-document"}:
                index += 1
                if index < len(tokens):
                    output = Path(tokens[index])
                index += 1
                continue
            if token.startswith("-"):
                index += 1
                continue
            if url is None:
                url = token
            index += 1

        if url is None or output is None:
            raise ValueError(
                f"Invalid wget command on line {line_number}: {' '.join(tokens)}"
            )

        return _WgetCommand(url=url, output_path=output, username=username, password=password)

    def _ensure_archive(self, command: _WgetCommand) -> Path:
        target_path = self._download_dir / command.output_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if target_path.exists():
            return target_path

        if not self.config.download_missing:
            raise FileNotFoundError(
                f"Required tranche archive is missing: {target_path}. "
                "Set download_missing=True to fetch automatically."
            )

        logger.info(
            "ingestion.zinc.download",
            source=self.config.name,
            url=command.url,
            output=str(target_path),
        )
        auth: tuple[str, str] | None = None
        if command.username and command.password:
            auth = (command.username, command.password)
        with httpx.stream("GET", command.url, auth=auth, timeout=60.0) as response:
            response.raise_for_status()
            with target_path.open("wb") as handle:
                for chunk in response.iter_bytes():
                    handle.write(chunk)
        return target_path

    def _iter_records(self, command: _WgetCommand) -> Iterator[MoleculeRecord]:
        archive_path = self._ensure_archive(command)
        delimiter = self.config.delimiter
        smiles_index = self.config.smiles_column
        identifier_index = self.config.identifier_column

        with gzip.open(archive_path, "rt", encoding="utf-8", errors="replace") as stream:
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
                        file=str(command.output_path),
                        line=line_number,
                    )
                    continue

                smiles = parts[smiles_index].strip()
                identifier = parts[identifier_index].strip()
                metadata = {
                    "source_file": str(command.output_path),
                    "download_url": command.url,
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
        commands = self._commands
        for command_index in range(start_entry, len(commands)):
            command = commands[command_index]
            line_offset = start_offset if command_index == start_entry else 0
            processed_lines = 0

            for record in self._iter_records(command):
                if processed_lines < line_offset:
                    processed_lines += 1
                    continue

                batch.append(record)
                processed_lines += 1
                if len(batch) >= self.config.batch_size:
                    next_cursor = {
                        "entry_index": command_index,
                        "line_offset": processed_lines,
                        "file": str(command.output_path),
                    }
                    yield IngestionPage(records=list(batch), next_cursor=next_cursor)
                    batch.clear()

            if line_offset and processed_lines < line_offset:
                logger.warning(
                    "ingestion.zinc.offset_exceeds_file",
                    source=self.config.name,
                    file=str(command.output_path),
                    expected_offset=line_offset,
                    available_records=processed_lines,
                )
                line_offset = processed_lines

            start_offset = 0

            next_cursor: dict[str, int] | None
            if batch:
                if command_index + 1 < len(commands):
                    next_cursor = {"entry_index": command_index + 1, "line_offset": 0}
                else:
                    next_cursor = None
                yield IngestionPage(records=list(batch), next_cursor=next_cursor)
                batch.clear()
            elif processed_lines == 0:
                if command_index + 1 < len(commands):
                    next_cursor = {"entry_index": command_index + 1, "line_offset": 0}
                else:
                    next_cursor = None
                yield IngestionPage(records=[], next_cursor=next_cursor)

        if not commands:
            yield IngestionPage(records=[], next_cursor=None)

__all__ = ["ZincConfig", "ZincConnector"]

