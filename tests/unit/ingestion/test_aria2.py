from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from open_molecule_data_pipeline.ingestion.aria2 import Aria2Options, download_with_aria2


def test_download_with_aria2_invokes_runner(tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def fake_runner(command: list[str]) -> None:
        commands.append(command)

    target = tmp_path / "archive.dat"
    download_with_aria2(
        "https://example.test/archive.dat",
        target,
        runner=fake_runner,
        options=Aria2Options(connections=8, split=4, min_split_size="2M"),
    )

    assert target.parent.exists()
    assert len(commands) == 1
    command = commands[0]
    assert command[0] == "aria2c"
    assert f"--dir={target.parent}" in command
    assert f"--out={target.name}" in command
    assert "--max-connection-per-server=8" in command
    assert "--split=4" in command
    assert "--min-split-size=2M" in command


def test_skip_existing_file(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake_runner(command: list[str]) -> None:
        calls.append(command)

    target = tmp_path / "archive.bin"
    target.write_bytes(b"payload")

    download_with_aria2(
        "https://example.test/archive.bin",
        target,
        runner=fake_runner,
    )

    assert calls == []


def test_resume_zero_length_file(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake_runner(command: list[str]) -> None:
        calls.append(command)

    target = tmp_path / "partial.bin"
    target.touch()

    download_with_aria2(
        "https://example.test/archive.bin",
        target,
        runner=fake_runner,
    )

    assert len(calls) == 1
    assert "--continue=true" in calls[0]


def test_checksum_failure(tmp_path: Path) -> None:
    def failing_runner(command: list[str]) -> None:
        raise subprocess.CalledProcessError(returncode=1, cmd=command)

    with pytest.raises(subprocess.CalledProcessError):
        download_with_aria2(
            "https://example.test/archive.bin",
            tmp_path / "archive.bin",
            checksum=("md5", "deadbeef"),
            runner=failing_runner,
        )


def test_authentication_arguments(tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def fake_runner(command: list[str]) -> None:
        commands.append(command)

    download_with_aria2(
        "https://example.test/archive.bin",
        tmp_path / "archive.bin",
        username="user",
        password="pass",
        runner=fake_runner,
    )

    assert any("--http-user" in part for part in commands[0])
    assert "user" in commands[0]
    assert "pass" in commands[0]
