"""PubChem ingestion connector implementation."""

from __future__ import annotations

from pydantic import Field

from .common import BaseHttpConnector, HttpSourceConfig


class PubChemConfig(HttpSourceConfig):
    """Configuration defaults tailored for the PubChem REST API."""

    base_url: str = "https://pubchem.ncbi.nlm.nih.gov"
    endpoint: str = "rest/pug/compound/cid/JSON"
    batch_param: str = "page_size"
    cursor_param: str | None = "page"
    records_path: list[str] = Field(default_factory=lambda: ["results"])
    next_cursor_path: list[str] = Field(default_factory=lambda: ["pagination", "next_page"])
    id_field: str = "cid"
    smiles_field: str = "smiles"
    metadata_fields: list[str] = Field(
        default_factory=lambda: ["inchi_key", "molecular_formula", "canonical_name"]
    )


class PubChemConnector(BaseHttpConnector):
    """Connector that downloads SMILES data from PubChem."""

    config: PubChemConfig


__all__ = ["PubChemConfig", "PubChemConnector"]
