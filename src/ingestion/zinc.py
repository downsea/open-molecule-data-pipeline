"""ZINC database ingestion connector."""

from __future__ import annotations

from pydantic import Field

from .common import BaseHttpConnector, HttpSourceConfig


class ZincConfig(HttpSourceConfig):
    """Configuration defaults for downloading from the ZINC database."""

    base_url: str = "https://zinc15.docking.org"
    endpoint: str = "substances.json"
    batch_param: str = "limit"
    cursor_param: str | None = "offset"
    records_path: list[str] = Field(default_factory=lambda: ["substances"])
    next_cursor_path: list[str] = Field(default_factory=lambda: ["next"])
    id_field: str = "zinc_id"
    smiles_field: str = "smiles"
    metadata_fields: list[str] = Field(default_factory=lambda: ["mwt", "logp", "reactivity"])


class ZincConnector(BaseHttpConnector):
    """Connector for paginated SMILES ingestion from ZINC."""

    config: ZincConfig


__all__ = ["ZincConfig", "ZincConnector"]
