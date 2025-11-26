from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from infrastructure.logging.logger import get_logger
from services.job_runner import JobRunner

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Kaggle ingestion job")
    parser.add_argument(
        "--job",
        dest="job_name",
        required=True,
        help="Name of the job defined in config/kaggle.yaml",
    )
    parser.add_argument(
        "--workspace",
        type=Path,
        default=Path("data/tmp"),
        help="Temporary directory used for downloads",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    runner = JobRunner()
    logger.info("Starting job %s", args.job_name)
    uploaded_objects = runner.run(args.job_name)
    logger.info("Uploaded objects: %s", uploaded_objects)


if __name__ == "__main__":
    main()

