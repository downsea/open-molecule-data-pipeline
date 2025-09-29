from __future__ import annotations

import gzip
import json
from pathlib import Path

from open_molecule_data_pipeline.ingestion.common import (
    CheckpointManager,
    IngestionCheckpoint,
    MoleculeRecord,
    NDJSONWriter,
)



def test_checkpoint_manager_roundtrip(tmp_path: Path) -> None:
    manager = CheckpointManager(tmp_path)
    checkpoint = IngestionCheckpoint(cursor={"cursor": "abc"}, batch_index=3, completed=True)
    manager.store("pubchem", checkpoint)

    loaded = manager.load("pubchem")
    assert loaded is not None
    assert loaded.cursor == {"cursor": "abc"}
    assert loaded.batch_index == 3
    assert loaded.completed is True


def test_ndjson_writer_writes_gzip(tmp_path: Path) -> None:
    writer = NDJSONWriter(tmp_path, compress=True)
    records = [
        MoleculeRecord(source="pubchem", identifier="CID1", smiles="C", metadata={"mass": 12.0}),
        MoleculeRecord(source="pubchem", identifier="CID2", smiles="CC", metadata={}),
    ]

    output = writer.write_batch("pubchem", batch_index=1, records=records)
    assert output.exists()

    with gzip.open(output, "rt", encoding="utf-8") as fh:
        lines = [json.loads(line) for line in fh]

    assert [line["identifier"] for line in lines] == ["CID1", "CID2"]
