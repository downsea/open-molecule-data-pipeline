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

## Continuous Integration
The repository ships with a GitHub Actions workflow (`.github/workflows/ci.yml`) that installs dependencies via `uv`, runs linting, type checking, and executes the test suite to ensure changes remain healthy.

## Next Steps
Stage 1 scaffolds the environment and repository structure. Subsequent stages will implement ingestion connectors, the resilient pipeline engine, analysis modules, and reporting capabilities.
