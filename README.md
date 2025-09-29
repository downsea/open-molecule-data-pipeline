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
│   ├── analysis/      # Molecular analysis modules
│   ├── common/        # Shared utilities and base classes
│   ├── ingestion/     # Connectors for public chemical databases
│   ├── pipeline/      # Execution engine and checkpointing logic
│   └── reporting/     # Report and dashboard generation
└── tests/
    ├── integration/   # Cross-module integration suites
    └── unit/          # Unit tests mirroring source packages
```

## Running SMILES Ingestion Jobs

Stage 2 introduces configurable ingestion connectors for ZINC, PubChem, ChEMBL, and ChemSpider along with checkpointed output writers. Jobs are described with YAML files (see [`config/ingestion-example.yaml`](config/ingestion-example.yaml)) and executed via the `smiles` CLI:

```bash
# Execute an ingestion job
uv run smiles ingest --config config/ingestion-example.yaml
```

Each source writes gzip-compressed NDJSON batches to `data/raw/<source>/` and maintains resumable checkpoints under `data/checkpoints/ingestion/<source>.json`. The sample configuration caches PubChem, ZINC, and ChEMBL archives under `data/raw/` and enables automatic resumption of interrupted transfers. To resume an interrupted job, re-run the same command; completed sources will be skipped automatically.

### Source-specific notes

- **ZINC** – Generate a tranche wget script from [CartBlanche](https://cartblanche.docking.org/tranches/2d) and save it as `data/ZINC22-downloader-2D-smi.gz.wget`. The connector parses each `wget` invocation, extracts embedded credentials, and shells out to `aria2c` to fetch any missing tranche archives (resume and checksum support included). Existing `.smi.gz` archives are reused directly; set `download_missing: true` (the example default) to have the CLI retrieve absent files automatically.
- **PubChem** – Download the HTML directory index from [`https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/`](https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/) and store it as `data/Index_of_pubchem_Compound_CURRENT-Full_SDF.html`. The connector reads this index, matches each `.sdf.gz` link with its companion `.md5` checksum, and uses `aria2c` to download or resume the bundles into the configured cache directory before parsing. All manifest and checksum files are parsed with UTF-8 fallbacks so that extended characters from mirrored FTP listings do not interrupt ingestion.
- **ChEMBL** – Record the bulk SDF URLs (one per line) in `data/chEMBL_sdf_link.txt`. Each archive is downloaded via `aria2c` (with resume) into the local cache prior to SMILES extraction. Default tag mappings expect `ChEMBL_ID` and `CANONICAL_SMILES`, but they can be overridden in the connector configuration.

## Continuous Integration
The repository ships with a GitHub Actions workflow (`.github/workflows/ci.yml`) that installs dependencies via `uv`, runs linting, type checking, and executes the test suite to ensure changes remain healthy.

## Next Steps
With ingestion connectors in place, Stage 3 will focus on the resilient processing pipeline, distributed execution, and deeper analysis integrations.
