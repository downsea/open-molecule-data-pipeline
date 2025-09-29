"""Shared helpers for parsing Structure Data Files (SDF)."""

from __future__ import annotations

import gzip
from collections.abc import Iterable, Iterator
from pathlib import Path


def _open_sdf(path: Path) -> Iterator[str]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as stream:
            for line in stream:
                yield line.rstrip("\n")
    else:
        with path.open("r", encoding="utf-8", errors="replace") as stream:
            for line in stream:
                yield line.rstrip("\n")


def _parse_entry(lines: Iterable[str]) -> dict[str, str]:
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


def iter_sdf_records(path: Path) -> Iterator[dict[str, str]]:
    """Yield property dictionaries for each SDF record in *path*."""

    lines: list[str] = []
    for line in _open_sdf(path):
        if line.strip() == "$$$$":
            if lines:
                yield _parse_entry(lines)
                lines = []
        else:
            lines.append(line)

    if lines:
        yield _parse_entry(lines)


__all__ = ["iter_sdf_records"]

