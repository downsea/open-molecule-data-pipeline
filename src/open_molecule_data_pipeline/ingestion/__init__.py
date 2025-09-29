"""Ingestion package exposing SMILES source connectors and orchestration tools."""

from .chembl import ChEMBLConfig, ChEMBLConnector
from .chemspider import ChemSpiderConfig, ChemSpiderConnector
from .common import (
    BaseConnector,
    BaseHttpConnector,
    CheckpointManager,
    HttpSourceConfig,
    IngestionCheckpoint,
    IngestionPage,
    MoleculeRecord,
    NDJSONWriter,
    SourceConfig,
)
from .pubchem import PubChemConfig, PubChemConnector
from .runner import IngestionJobConfig, SourceDefinition, load_config, run_ingestion
from .zinc import ZincConfig, ZincConnector

__all__ = [
    "BaseConnector",
    "BaseHttpConnector",
    "CheckpointManager",
    "HttpSourceConfig",
    "IngestionCheckpoint",
    "IngestionJobConfig",
    "IngestionPage",
    "MoleculeRecord",
    "NDJSONWriter",
    "SourceConfig",
    "SourceDefinition",
    "load_config",
    "run_ingestion",
    "ChEMBLConfig",
    "ChEMBLConnector",
    "ChemSpiderConfig",
    "ChemSpiderConnector",
    "PubChemConfig",
    "PubChemConnector",
    "ZincConfig",
    "ZincConnector",
]
