"""ChEMBL ingestion connector."""

from __future__ import annotations

from pydantic import Field

from .common import BaseHttpConnector, HttpSourceConfig


class ChEMBLConfig(HttpSourceConfig):
    """Configuration defaults for the ChEMBL API."""

    base_url: str = "https://www.ebi.ac.uk"
    endpoint: str = "chembl/api/data/molecule.json"
    batch_param: str = "limit"
    cursor_param: str | None = "offset"
    records_path: list[str] = Field(default_factory=lambda: ["molecules"])
    next_cursor_path: list[str] = Field(default_factory=lambda: ["page_meta", "next"])
    id_field: str = "molecule_chembl_id"
    smiles_field: str = "canonical_smiles"
    metadata_fields: list[str] = Field(
        default_factory=lambda: ["pref_name", "molecule_type", "molecular_weight"]
    )


class ChEMBLConnector(BaseHttpConnector):
    """Connector for ingesting SMILES data from ChEMBL."""

    config: ChEMBLConfig


__all__ = ["ChEMBLConfig", "ChEMBLConnector"]
