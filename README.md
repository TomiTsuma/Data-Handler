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
2. Choose one of the execution modes:
   - **Managed job (YAML-driven)**:
     1. Update `config/kaggle.yaml` with the datasets and MinIO bucket you want to use.
     2. Run `python scripts/run_ingestion.py --job housing_price_index`.
   - **Ad-hoc dataset (CLI-only)**:
     1. Run `python -m scripts.run_ingestion --dataset-id kundanbedmutha/instagram-analytics-dataset --bucket kaggle-raw --prefix instagram`.
     2. Optionally limit files: append `--files file1.csv file2.csv`.
3. Downloads land in `data/tmp/<job_id>` and are uploaded to MinIO under the chosen bucket/prefix.

## Project Structure

- `config/` - Configuration files for data sources
- `infrastructure/` - Storage, logging, and database utilities
- `core/` - Models, utilities, and exceptions
- `ingestion/` - Ingestion method implementations
- `services/` - API and orchestration services
- `scripts/` - Utility scripts
- `tests/` - Test suites
- `data/` - Local data storage (raw, processed, tmp)
