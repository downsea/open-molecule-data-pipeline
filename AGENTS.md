# Agent Instructions

## Scope
These guidelines apply to the entire repository until another `AGENTS.md` in a subdirectory overrides them.

## Tooling & Environment
- Manage dependencies with `uv`. Always update `pyproject.toml` and `uv.lock` together and use `uv sync` in documentation and scripts for environment setup.
- Configure formatting with `ruff` (for linting/formatting) and `black` compatibility; ensure pre-commit hooks or CI leverage `uv run` for commands.

## Project Structure
- Place all Python source code under the `src/open_molecule_data_pipeline/` namespace package. Create subpackages (e.g., `ingestion`, `pipeline`, `analysis`, `reporting`) within that namespace rather than adding new top-level packages under `src/`.
- Keep configuration files in `config/` (YAML or TOML) and store reusable notebooks or experiments in `notebooks/`.
- Persist large data artifacts in `data/` with subfolders `raw/`, `processed/`, and `checkpoints/`; avoid committing large binaries.
- Maintain ingestion manifests under `data/` (for example `data/ZINC-downloader-2D-txt.uri` for ZINC tranche downloads) so connectors can mirror external archives without API access.

## Coding Conventions
- Use type annotations and `pydantic` models for configuration and data schemas.
- Implement retry logic with configurable backoff for all external API calls.
- Provide docstrings for public classes/functions outlining parameters, returns, and exceptions.
- Write pipeline stages as pure, testable functions or classes that accept dependency-injected resources (e.g., clients, storage handlers).

## Testing & Quality
- Place unit tests in `tests/unit/` and integration tests in `tests/integration/` mirroring module structure.
- Ensure integration tests mock external services or use recorded fixtures; avoid live API calls by default.
- Use `uv run` to execute `pytest`, `ruff`, and other tooling commands.

## Documentation
- Maintain high-level architecture documentation under `docs/` (e.g., pipeline diagrams, ingestion specs).
- Update `README.md` when new CLI commands or setup steps are added, emphasizing `uv sync` usage.

