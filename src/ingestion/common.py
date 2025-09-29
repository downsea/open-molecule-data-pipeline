"""Shared utilities and models for SMILES ingestion connectors."""

from __future__ import annotations

import gzip
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from pathlib import Path
from typing import Any

import httpx
import orjson
import structlog
from pydantic import BaseModel, Field
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = structlog.get_logger(__name__)


DEFAULT_USER_AGENT = "open-molecule-data-pipeline/0.1"


class MoleculeRecord(BaseModel):
    """Canonical representation for downloaded SMILES entries."""

    source: str
    identifier: str
    smiles: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestionPage(BaseModel):
    """A single paginated response from a source connector."""

    records: list[MoleculeRecord]
    next_cursor: MutableMapping[str, Any] | None = None


class IngestionCheckpoint(BaseModel):
    """Serialized checkpoint data for a connector."""

    cursor: MutableMapping[str, Any] = Field(default_factory=dict)
    batch_index: int = 0
    completed: bool = False


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Write JSON to *path* atomically to avoid partial checkpoint updates."""

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path.write_bytes(orjson.dumps(payload))
    tmp_path.replace(path)


class CheckpointManager:
    """Manage reading and writing checkpoints for ingestion connectors."""

    def __init__(self, base_path: Path) -> None:
        self._base_path = base_path
        self._base_path.mkdir(parents=True, exist_ok=True)

    def load(self, source: str) -> IngestionCheckpoint | None:
        """Load the checkpoint for *source* if it exists."""

        path = self._base_path / f"{source}.json"
        if not path.exists():
            return None
        data = orjson.loads(path.read_bytes())
        return IngestionCheckpoint.model_validate(data)

    def store(self, source: str, checkpoint: IngestionCheckpoint) -> None:
        """Persist *checkpoint* for *source* atomically."""

        path = self._base_path / f"{source}.json"
        _atomic_write_json(path, checkpoint.model_dump())


class HttpError(RuntimeError):
    """Raised when a connector encounters an unrecoverable HTTP error."""


def build_http_client(
    timeout: float = 30.0, headers: Mapping[str, str] | None = None
) -> httpx.Client:
    """Create a configured :class:`httpx.Client` with retry-friendly defaults."""

    merged_headers = {"User-Agent": DEFAULT_USER_AGENT}
    if headers:
        merged_headers.update(headers)
    return httpx.Client(timeout=timeout, headers=merged_headers)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    retry=retry_if_exception_type(httpx.HTTPError),
)
def execute_request(client: httpx.Client, request: httpx.Request) -> httpx.Response:
    """Execute *request* with retry and raise :class:`HttpError` on HTTP failures."""

    response = client.send(request)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:  # pragma: no cover - defensive
        raise HttpError(str(exc)) from exc
    return response


def extract_json_path(payload: Any, path: Iterable[str]) -> Any:
    """Traverse *payload* following *path* keys, returning ``None`` if absent."""

    current = payload
    for key in path:
        if current is None:
            return None
        if isinstance(current, Mapping):
            current = current.get(key)
        else:  # pragma: no cover - guard for unexpected payloads
            return None
    return current


class SourceConfig(BaseModel):
    """Base configuration shared by all ingestion connectors."""

    name: str
    batch_size: int = Field(default=1000, gt=0)


class HttpSourceConfig(SourceConfig):
    """Configuration shared by HTTP-backed ingestion sources."""

    base_url: str
    endpoint: str
    batch_param: str = "batch_size"
    cursor_param: str | None = "cursor"
    params: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    start_cursor: MutableMapping[str, Any] = Field(default_factory=dict)
    records_path: list[str] = Field(default_factory=lambda: ["records"])
    next_cursor_path: list[str] = Field(default_factory=lambda: ["next"])
    id_field: str = "id"
    smiles_field: str = "smiles"
    metadata_fields: list[str] = Field(default_factory=list)


class BaseConnector:
    """Abstract base class for ingestion connectors."""

    def __init__(self, config: SourceConfig, checkpoint_manager: CheckpointManager) -> None:
        self.config = config
        self._checkpoint_manager = checkpoint_manager

    def fetch_pages(self) -> Iterator[IngestionPage]:
        """Yield pages of ingestion results."""

        raise NotImplementedError

    def close(self) -> None:  # pragma: no cover - interface default
        """Release any connector resources."""


class BaseHttpConnector(BaseConnector):
    """Base implementation for connectors backed by paginated HTTP APIs."""

    def __init__(
        self,
        config: HttpSourceConfig,
        checkpoint_manager: CheckpointManager,
        client_factory: Callable[[Mapping[str, str]], httpx.Client] | None = None,
    ) -> None:
        super().__init__(config=config, checkpoint_manager=checkpoint_manager)
        factory = client_factory or (lambda headers: build_http_client(headers=headers))
        self._client = factory(config.headers)

    def _build_request(self, cursor: MutableMapping[str, Any]) -> httpx.Request:
        params: dict[str, Any] = {**self.config.params}
        params.update(cursor)
        if self.config.batch_param:
            params[self.config.batch_param] = self.config.batch_size
        url = f"{self.config.base_url.rstrip('/')}/{self.config.endpoint.lstrip('/')}"
        return self._client.build_request("GET", url, params=params)

    def _parse_records(self, payload: Mapping[str, Any]) -> list[MoleculeRecord]:
        raw_records = extract_json_path(payload, self.config.records_path) or []
        records: list[MoleculeRecord] = []
        for item in raw_records:
            if not isinstance(item, Mapping):  # pragma: no cover - guard clause
                continue
            identifier = str(item.get(self.config.id_field, ""))
            smiles = str(item.get(self.config.smiles_field, ""))
            metadata = {
                key: item.get(key)
                for key in (self.config.metadata_fields or item.keys())
                if key not in {self.config.id_field, self.config.smiles_field}
            }
            records.append(
                MoleculeRecord(
                    source=self.config.name,
                    identifier=identifier,
                    smiles=smiles,
                    metadata=dict(metadata),
                )
            )
        return records

    def _next_cursor(self, payload: Mapping[str, Any]) -> MutableMapping[str, Any] | None:
        value = extract_json_path(payload, self.config.next_cursor_path)
        if value is None:
            return None
        if isinstance(value, Mapping):
            return dict(value)
        if self.config.cursor_param:
            return {self.config.cursor_param: value}
        return None

    def fetch_pages(self) -> Iterator[IngestionPage]:
        """Yield :class:`IngestionPage` instances until the source is exhausted."""

        checkpoint = self._checkpoint_manager.load(self.config.name)
        if checkpoint and checkpoint.completed:
            logger.info("ingestion.skip", source=self.config.name, reason="completed")
            return

        next_cursor: MutableMapping[str, Any] = (
            dict(checkpoint.cursor) if checkpoint else dict(self.config.start_cursor)
        )
        while True:
            request = self._build_request(next_cursor)
            response = execute_request(self._client, request)
            payload = response.json()
            records = self._parse_records(payload)
            cursor_state = self._next_cursor(payload)
            logger.info(
                "ingestion.page",
                source=self.config.name,
                records=len(records),
                next_cursor=cursor_state,
            )
            yield IngestionPage(records=records, next_cursor=cursor_state)
            if not cursor_state:
                break
            next_cursor = cursor_state

    def close(self) -> None:
        """Close the underlying HTTP client."""

        self._client.close()


class NDJSONWriter:
    """Persist ingestion batches to newline-delimited JSON files."""

    def __init__(self, base_dir: Path, compress: bool = True) -> None:
        self._base_dir = base_dir
        self._compress = compress
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def write_batch(self, source: str, batch_index: int, records: Iterable[MoleculeRecord]) -> Path:
        """Write *records* to disk and return the generated file path."""

        suffix = ".jsonl.gz" if self._compress else ".jsonl"
        filename = f"{source}-batch-{batch_index:06d}{suffix}"
        path = self._base_dir / source / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        if self._compress:
            with gzip.open(path, "wt", encoding="utf-8") as fh:
                for record in records:
                    fh.write(record.model_dump_json())
                    fh.write("\n")
        else:
            with path.open("w", encoding="utf-8") as fh:
                for record in records:
                    fh.write(record.model_dump_json())
                    fh.write("\n")
        return path


__all__ = [
    "BaseConnector",
    "BaseHttpConnector",
    "CheckpointManager",
    "HttpError",
    "HttpSourceConfig",
    "IngestionCheckpoint",
    "IngestionPage",
    "MoleculeRecord",
    "NDJSONWriter",
    "SourceConfig",
    "build_http_client",
    "execute_request",
]
