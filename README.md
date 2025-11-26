# Data Ingestion + MinIO Storage Service

This service provides scalable data ingestion pipelines with MinIO storage for a homelab setup, designed for extensibility into a full data platform.

## Features

- **Multiple Ingestion Methods**: Kaggle, REST APIs, Web Scraping, S3, Python Downloads
- **MinIO Storage**: Unified object storage with versioning and lifecycle management
- **Pipeline Architecture**: Modular, extensible ingestion pipelines
- **FastAPI Service**: REST API for triggering ingestion jobs
- **Configuration-Driven**: YAML-based configuration for data sources
- **Logging & Monitoring**: Comprehensive logging and metadata tracking

## Quick Start

See `docker-compose.yml` for local development setup.

### Kaggle → MinIO pipeline

1. Copy `.env.example` to `.env` and set the following variables:
   - `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_DEFAULT_BUCKET`, `MINIO_REGION`, `MINIO_USE_SSL`
   - `KAGGLE_USERNAME`, `KAGGLE_KEY` (mirrors `~/.kaggle/kaggle.json`)
   - Optional defaults: `KAGGLE_DATASET_OWNER`, `KAGGLE_DATASET_SLUG`, `KAGGLE_DATASET_FILES`
2. Update `config/kaggle.yaml` with the datasets and MinIO bucket you want to use.
3. Run the runner script: `python scripts/run_ingestion.py --job housing_price_index`.
4. The files will be downloaded into `data/tmp/<job_name>` and uploaded to MinIO.

## Project Structure

- `config/` - Configuration files for data sources
- `infrastructure/` - Storage, logging, and database utilities
- `core/` - Models, utilities, and exceptions
- `ingestion/` - Ingestion method implementations
- `services/` - API and orchestration services
- `scripts/` - Utility scripts
- `tests/` - Test suites
- `data/` - Local data storage (raw, processed, tmp)
