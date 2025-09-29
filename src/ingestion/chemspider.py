"""ChemSpider ingestion connector."""

from __future__ import annotations

from pydantic import Field

from .common import BaseHttpConnector, HttpSourceConfig


class ChemSpiderConfig(HttpSourceConfig):
    """Configuration defaults for the ChemSpider API."""

    base_url: str = "https://api.rsc.org"
    endpoint: str = "compounds/v1/filter/smiles"
    batch_param: str = "count"
    cursor_param: str | None = "token"
    records_path: list[str] = Field(default_factory=lambda: ["results"])
    next_cursor_path: list[str] = Field(default_factory=lambda: ["next"])
    id_field: str = "csid"
    smiles_field: str = "smiles"
    metadata_fields: list[str] = Field(default_factory=lambda: ["inchi_key", "formula"])


class ChemSpiderConnector(BaseHttpConnector):
    """Connector for retrieving SMILES strings from ChemSpider."""

    config: ChemSpiderConfig


__all__ = ["ChemSpiderConfig", "ChemSpiderConnector"]
