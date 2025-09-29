"""Utilities for invoking :mod:`aria2c` as the unified download backend."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Sequence

Runner = Callable[[Sequence[str]], object]


@dataclass(frozen=True)
class Aria2Options:
    """Configuration options passed to :command:`aria2c`."""

    connections: int = 16
    split: int = 16
    min_split_size: str = "1M"
    max_tries: int = 5
    retry_wait: int = 2
    extra_args: tuple[str, ...] = field(default_factory=tuple)

    def to_args(self) -> list[str]:
        """Return CLI arguments derived from the option values."""

        args = [
            f"--max-connection-per-server={self.connections}",
            f"--split={self.split}",
            f"--min-split-size={self.min_split_size}",
            f"--max-tries={self.max_tries}",
            f"--retry-wait={self.retry_wait}",
        ]
        args.extend(self.extra_args)
        return args


def _default_runner(command: Sequence[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(command, check=True, capture_output=True)


def download_with_aria2(
    url: str,
    output_path: Path,
    *,
    checksum: tuple[str, str] | None = None,
    username: str | None = None,
    password: str | None = None,
    skip_existing: bool = True,
    options: Aria2Options | None = None,
    runner: Runner | None = None,
) -> None:
    """Download *url* to *output_path* using :command:`aria2c`.

    Parameters
    ----------
    url:
        HTTP or FTP URL pointing to the payload.
    output_path:
        Target path for the downloaded file. Parent directories are created as
        needed.
    checksum:
        Optional tuple of ``(algorithm, value)`` passed to ``--checksum`` for
        integrity verification.
    username / password:
        Optional HTTP basic authentication credentials.
    skip_existing:
        If ``True`` (the default) and *output_path* exists with a positive size
        and no checksum validation is requested, the download is skipped.
    options:
        Advanced :class:`Aria2Options` controlling concurrency and retries.
    runner:
        Callable executing the constructed command. Defaults to
        :func:`subprocess.run` with ``check=True`` and output capture so unit
        tests can substitute their own implementation.
    """

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if skip_existing and output_path.exists() and output_path.stat().st_size > 0 and checksum is None:
        return

    command: list[str] = [
        "aria2c",
        "--continue=true",
        "--auto-file-renaming=false",
        "--allow-overwrite=true",
        f"--dir={output_path.parent}",
        f"--out={output_path.name}",
    ]

    opts = options or Aria2Options()
    command.extend(opts.to_args())

    if checksum is not None:
        algorithm, value = checksum
        command.append(f"--checksum={algorithm}={value}")
        command.append("--check-integrity=true")

    if username:
        command.extend(["--http-user", username])
    if password:
        command.extend(["--http-password", password])

    command.append(url)

    exec_runner = runner or _default_runner
    exec_runner(command)


__all__ = ["Aria2Options", "download_with_aria2"]

