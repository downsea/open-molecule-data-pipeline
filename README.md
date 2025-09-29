# Open Molecule Data Pipeline

A scalable toolkit for ingesting, processing, and analyzing large-scale collections of SMILES strings from public chemical databases.

## Project Goals
- High-throughput ingestion from sources such as ZINC, PubChem, ChEMBL, and ChemSpider.
- Resilient, restartable processing pipelines capable of handling billions of molecules.
- Configurable transformation and analysis stages driven by declarative configuration.
- Comprehensive reporting and observability for operational insight.

## Getting Started

### Prerequisites
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager installed and on your `PATH`.
- [`aria2c`](https://aria2.github.io/) command-line downloader available for resumable transfers.

### Environment Setup
```bash
# Install the project and synchronized dependencies
uv sync

# Include developer tooling (linters, tests)
uv sync --extra dev
```

All developer tooling is available through `uv` scripts:

```bash
# Format code
uv run ruff format

# Lint
uv run ruff check

# Static type checks
uv run mypy src

# Run tests
uv run pytest
```

### Repository Layout
```
├── config/            # Environment-specific configuration files (YAML/TOML)
├── data/
│   ├── raw/           # Source extracts and ingestion outputs
│   ├── processed/     # Intermediate pipeline artifacts
│   └── checkpoints/   # Persistent state for restartable jobs
├── docs/              # Architecture plans and design notes
├── src/
│   └── open_molecule_data_pipeline/
│       ├── analysis/  # Molecular analysis modules
│       ├── common/    # Shared utilities and base classes
│       ├── ingestion/ # Connectors, CLI, orchestration
│       ├── pipeline/  # Execution engine and checkpointing logic
│       └── reporting/ # Report and dashboard generation
└── tests/
    ├── integration/   # Cross-module integration suites
    └── unit/          # Unit tests mirroring source packages
```

## Running SMILES Ingestion Jobs

Stage 2 introduces configurable ingestion connectors for ZINC, PubChem, ChEMBL, and ChemSpider along with checkpointed output writers. Jobs are described with YAML files (see [`config/ingestion-example.yaml`](config/ingestion-example.yaml)) and executed via the `smiles` CLI:

```bash
# Phase 1: mirror raw archives referenced in the job definition
uv run smiles download --config config/ingestion-example.yaml

# Phase 2: parse cached archives and emit SMILES batches (streaming sources will
# run end-to-end in a single step)
uv run smiles ingest --config config/ingestion-example.yaml
```

Streaming connectors (for example ChemSpider) write gzip-compressed NDJSON batches to `data/raw/<source>/` and maintain resumable checkpoints under `data/checkpoints/ingestion/<source>.json`. Download-focused connectors (PubChem, ChEMBL, ZINC) instead ensure the referenced SDF archives are cached under `data/raw/` while marking the checkpoint as completed so future runs can skip already mirrored files. To resume an interrupted job, re-run the same command; completed sources will be skipped automatically.

Every successful ingestion run also generates a Markdown summary at `data/raw/raw-data-report.md`. The report captures per-source batch and record counts (where applicable), output file sizes, and any cached download artifacts so you can audit large transfers quickly.

### Source-specific notes

- **ZINC** – Export the tranche URI list from [ZINC CartBlanche](https://cartblanche.docking.org/tranches/home/) and save it as `data/ZINC-downloader-2D-txt.uri` (one HTTP/S URL per line). The connector mirrors each referenced tranche with `aria2c`, reusing any archives already present under `data/raw/zinc22/`. Provide `username` / `password` in the configuration if your manifest requires authenticated downloads, and set `download_missing: true` to fetch absent files automatically.
- **PubChem** – Extract the direct `.sdf.gz` URLs from the FTP listing (for example using `curl` or your browser) and record them one per line in `data/pubchem_sdf_link.txt`. The connector reads this manifest, appends `.md5` to each entry to fetch the companion checksum, and shells out to `aria2c` for resumable, multi-connection downloads before parsing the cached archives. Manifest and checksum files are parsed with UTF-8 fallbacks so mirrored listings with extended characters do not interrupt ingestion.
- **ChEMBL** – Record the bulk SDF URLs (one per line) in `data/chEMBL_sdf_link.txt`. Each archive is downloaded via `aria2c` (with resume) into the local cache prior to SMILES extraction. Default tag mappings expect `ChEMBL_ID` and `CANONICAL_SMILES`, but they can be overridden in the connector configuration.

## Continuous Integration
The repository ships with a GitHub Actions workflow (`.github/workflows/ci.yml`) that installs dependencies via `uv`, runs linting, type checking, and executes the test suite to ensure changes remain healthy.

## Next Steps
With ingestion connectors in place, Stage 3 will focus on the resilient processing pipeline, distributed execution, and deeper analysis integrations.
