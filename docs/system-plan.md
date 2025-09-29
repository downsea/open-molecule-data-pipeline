# SMILES Processing System Implementation Plan

## 1. Foundation & Environment Setup *(Completed)*
- **Dependency Management**: ✅ `pyproject.toml` defines core runtime dependencies (`httpx`, `pydantic`, `polars`, `rdkit-pypi`, `tenacity`, `pyyaml`, `click`, `orjson`, `uvicorn`, `structlog`) with documented commands for linting (`uv run ruff check`), formatting (`uv run ruff format`), type checking (`uv run mypy src`), and testing (`uv run pytest`).
- **Project Layout**: ✅ Repository scaffolded with `src/` packages (`ingestion`, `pipeline`, `analysis`, `reporting`, `common`), configuration directory (`config/`), and artifact storage directories under `data/` (`raw/`, `processed/`, `checkpoints/`).
- **Continuous Integration**: ✅ GitHub Actions workflow leverages `uv` to sync dependencies, then executes linting, type checking, and tests on push and pull requests.
- **Documentation**: ✅ `README.md` now documents `uv sync` environment setup, the repository layout, and developer tooling quickstart commands.

## 2. Data Ingestion Layer *(Completed)*
- **Phase 1 – Raw Archive Mirroring**: ✅ `smiles download --config config/ingestion-example.yaml` hydrates the `data/raw/` cache by streaming SDF/SMI tranche archives from URI manifests (ZINC) or bulk download lists (PubChem, ChEMBL). Checkpoints mark successful downloads so re-runs skip existing files.
- **Phase 2 – SMILES Parsing & Ingestion**: ✅ Follow-up parsing jobs (stage design in progress) read the cached archives, normalize source-specific payloads, and emit `(identifier, smiles)` pairs into the data store. Format adapters must handle gzipped SMILES tables, SDF property blocks, and future formats.
- **Shared Utilities**: ✅ `src/open_molecule_data_pipeline/ingestion/common.py` centralizes HTTP client creation, retry logic, checkpoint persistence, and gzip-compressed NDJSON writers backed by `pydantic` models.
- **Checkpointing**: ✅ Checkpoints live under `data/checkpoints/ingestion/` with atomic writes and completion flags allowing download or parsing runs to resume or skip completed sources.
- **Ingestion Orchestrator**: ✅ `smiles injestion --config config/ingestion-example.yaml` (phase 1) and the forthcoming parsing CLI share orchestration helpers for concurrency, reporting, and raw-data Markdown summaries.

## 3. Processing Pipeline Engine
- **Configuration Schema**: Define YAML schema describing stages, dependencies, resource limits, and output sinks. Validate with `pydantic` models (`PipelineConfig`, `StageConfig`).
- **Execution Core**: Build DAG executor supporting stage types (map, reduce, filter) with multiprocessing via `multiprocessing` or `ray` backend toggle. Include dynamic batching to process billions of SMILES using memory-efficient iterators and chunked I/O.
- **Checkpointing**: Implement `pipeline/checkpoint.py` to store stage progress (batch IDs, offsets, partial results). Use atomic writes and versioned snapshots for safe resume.
- **Resilience**: Provide retry policies, dead-letter queues for failed molecules, and health monitoring hooks (metrics, logs). Ensure graceful shutdown flushes checkpoints.
- **CLI & Services**: Add `smiles pipeline run` to execute configured pipelines, `smiles pipeline resume` to restart from checkpoints, and optional REST control plane using `FastAPI` for orchestration.

## 4. SMILES Transformation & Analysis
- **Standardization**: Implement modules for canonicalization, neutralization, salt stripping, and filtering (e.g., Lipinski, PAINS). Use RDKit wrappers with configurable parameters.
- **Feature Extraction**: Compute descriptors (molecular weight, logP, TPSA), fingerprints (Morgan, MACCS), and structural alerts. Store results in columnar formats via `polars`/`pyarrow`.
- **Quality Assurance**: Validation stage to detect invalid SMILES, duplicates, or missing data. Integrate metrics collection for error rates and throughput.
- **Analysis Outputs**: Aggregate statistics per batch and globally, producing JSON summaries, CSV tables, and optional parquet datasets for downstream ML.

## 5. Reporting & Observability
- **Reporting Engine**: Generate templated Markdown/HTML/PDF reports summarizing ingestion counts, processing success rates, descriptor distributions, and configuration metadata.
- **Dashboards**: Integrate with `rich`/`textual` CLI dashboards or export metrics to Prometheus/Grafana using `prometheus-client`.
- **Lineage & Metadata**: Track provenance (source, timestamp, config hash) in metadata store. Include audit logs for compliance.

## 6. Scalability & Operations
- **Horizontal Scaling**: Provide Kubernetes manifests and Helm charts for deploying ingestion and processing workers. Support distributed queues (Kafka, Redis Streams) for decoupled ingestion and processing.
- **Cloud Storage Integration**: Abstract storage layer to support local filesystem, S3, GCS, and Azure. Use async I/O where available to maximize throughput.
- **Load Testing**: Create synthetic dataset generator to benchmark throughput (target: billions of SMILES). Automate performance tests with `pytest-benchmark` or custom harness.
- **Disaster Recovery**: Implement periodic snapshotting of checkpoints and metadata to remote storage. Include validation scripts to ensure snapshots are restorable.

## 7. Roadmap & Milestones
1. **Milestone 1**: Project scaffolding, environment setup, and ingestion of a single source with checkpointing (2–3 weeks).
2. **Milestone 2**: Full multi-source ingestion with unified schema, pipeline engine MVP with multiprocessing, and basic analysis modules (4–6 weeks).
3. **Milestone 3**: Distributed execution, advanced reporting, observability stack, and deployment automation (6–8 weeks).
4. **Milestone 4**: Performance hardening for billion-scale workloads, compliance features, and extensive documentation (ongoing).

## 8. Risks & Mitigations
- **API Rate Limits**: Mitigate with adaptive throttling, caching, and parallelization across multiple API keys/accounts.
- **Data Quality Issues**: Employ rigorous validation, fallback sources, and manual review workflows.
- **Operational Complexity**: Provide infrastructure-as-code templates, clear runbooks, and automation for routine tasks.
- **Dependency on RDKit**: Offer optional lightweight parsing using `openbabel` or `rdkit-lite` and isolate heavy dependencies to specialized workers.

