"""
ArXiv Paper Downloader & MinIO Uploader
========================================
Downloads CS papers matching:
  AND all="de novo" AND all="diffusion" AND all="molecule"
from 2022-01-01 to 2026-12-31, then uploads PDFs to a MinIO bucket.

Dependencies:
    pip install arxiv minio tqdm

Configuration:
    Set the MinIO connection variables in the CONFIG section below,
    or pass them via environment variables:
        MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
        MINIO_BUCKET, MINIO_SECURE (optional, default "false")
"""

import os
import time
import logging
import hashlib
import argparse
from pathlib import Path
from datetime import datetime, timezone

import arxiv
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm

# ---------------------------------------------------------------------------
# CONFIG  ← edit here or override with env vars
# ---------------------------------------------------------------------------
MINIO_ENDPOINT   = "100.127.65.29:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_BUCKET     = "arxiv-papers"
MINIO_SECURE     = False
MINIO_PREFIX     = "de-novo-diffusion-molecule/"

DOWNLOAD_DIR     = Path("/home/rhadamanthys/Data-Handler/data/tmp/de-novo-diffusion-molecule")

# ArXiv query parameters
SEARCH_TERMS = ['all:"de novo"', 'all:"diffusion"', 'all:"molecule"']
DATE_FROM    = "2022-01-01"
DATE_TO      = "2026-12-31"
MAX_RESULTS  = 50          # arxiv library 'max_results' per query
INCLUDE_CROSS_LIST = True  # handled via category filter below

# Retry settings
MAX_RETRIES  = 3
RETRY_DELAY  = 5           # seconds between retries
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("arxiv_downloader.log"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ArXiv helpers
# ---------------------------------------------------------------------------

def build_query() -> str:
    """
    Build a query string that mirrors the portal filter:
      classification: Computer Science (cs), include_cross_list: True
      terms: AND all="de novo" AND all="diffusion" AND all="molecule"
      date_range: 2022-01-01 to 2026-12-31
    """
    terms_part = " AND ".join(SEARCH_TERMS)

    # Date range via submittedDate field (format: YYYYMMDDHHMMSS)
    date_from_fmt = DATE_FROM.replace("-", "") + "000000"
    date_to_fmt   = DATE_TO.replace("-",   "") + "235959"
    date_part = f"submittedDate:[{date_from_fmt} TO {date_to_fmt}]"

    # CS classification; cross-list papers naturally appear when the
    # cs.* category is one of the listed categories on the paper.
    cat_part = "cat:cs.*"

    query = f"({terms_part}) AND {date_part} AND {cat_part}"
    log.info("ArXiv query: %s", query)
    return query


def fetch_papers(query: str) -> list[arxiv.Result]:
    """Run the search and return results sorted by announced date (newest first)."""
    client = arxiv.Client(
        page_size=MAX_RESULTS,
        delay_seconds=3,        # be polite to the API
        num_retries=MAX_RETRIES,
    )
    search = arxiv.Search(
        query=query,
        max_results=MAX_RESULTS,
        sort_by=arxiv.SortCriterion.SubmittedDate,
        sort_order=arxiv.SortOrder.Descending,
    )
    results = list(client.results(search))
    log.info("Fetched %d papers from ArXiv.", len(results))
    return results


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def sanitize_filename(paper: arxiv.Result) -> str:
    """Return a clean filename: <arxiv_id>_<slug_title>.pdf"""
    arxiv_id = paper.get_short_id().replace("/", "_")
    slug = paper.title[:60].strip()
    slug = "".join(c if c.isalnum() or c in " -_" else "_" for c in slug)
    slug = "_".join(slug.split())           # collapse whitespace
    return f"{arxiv_id}_{slug}.pdf"


def download_paper(paper: arxiv.Result, dest_dir: Path) -> Path | None:
    """Download a single paper PDF; returns local path or None on failure."""
    filename = sanitize_filename(paper)
    dest_path = dest_dir / filename

    if dest_path.exists():
        log.info("Already downloaded: %s", filename)
        return dest_path

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            paper.download_pdf(dirpath=str(dest_dir), filename=filename)
            log.info("Downloaded: %s", filename)
            return dest_path
        except Exception as exc:
            log.warning("Attempt %d/%d failed for %s: %s",
                        attempt, MAX_RETRIES, filename, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    log.error("Failed to download after %d attempts: %s", MAX_RETRIES, filename)
    return None


# ---------------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------------

def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info("Created MinIO bucket: %s", bucket)
    else:
        log.info("MinIO bucket exists: %s", bucket)


def md5_of_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def sanitize_metadata_value(value: str) -> str:
    """Strip or replace non-ASCII characters for MinIO metadata compatibility."""
    return value.encode("ascii", errors="replace").decode("ascii").strip()

import unicodedata

def to_ascii(value: str) -> str:
    return (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
    )

def upload_to_minio(client: Minio, local_path: Path, paper: arxiv.Result) -> bool:
    """Upload a PDF to MinIO; returns True on success."""
    object_name = MINIO_PREFIX + sanitize_metadata_value(local_path.name)
    print(object_name)

    # Check if already uploaded (compare ETag/md5)
    try:
        stat = client.stat_object(MINIO_BUCKET, object_name)
        remote_etag = stat.etag.strip('"')
        local_md5   = md5_of_file(local_path)
        if remote_etag == local_md5:
            log.info("Already in MinIO (unchanged): %s", object_name)
            return True
        log.info("Re-uploading changed file: %s", object_name)
    except S3Error as e:
        if e.code != "NoSuchKey":
            log.warning("stat_object error for %s: %s", object_name, e)

    # Build metadata tags
    metadata = {
        "arxiv-id":    to_ascii(str(paper.get_short_id())),
        "title":       to_ascii(paper.title[:200]),
        "published":   to_ascii(paper.published.isoformat()) if paper.published else "",
        "primary-cat": to_ascii(str(paper.primary_category)),
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client.fput_object(
                MINIO_BUCKET,
                object_name,
                str(local_path),
                content_type="application/pdf",
                metadata=metadata,
            )
            log.info("Uploaded to MinIO: %s", object_name)
            return True
        except S3Error as exc:
            log.warning("Upload attempt %d/%d failed for %s: %s",
                        attempt, MAX_RETRIES, object_name, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    log.error("Failed to upload to MinIO: %s", object_name)
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download ArXiv papers and upload to MinIO.")
    p.add_argument("--download-dir", default=str(DOWNLOAD_DIR),
                   help="Local directory to save PDFs (default: ./arxiv_papers)")
    p.add_argument("--skip-upload", action="store_true",
                   help="Download only; skip MinIO upload")
    p.add_argument("--skip-download", action="store_true",
                   help="Upload existing files in download-dir without fetching from ArXiv")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    dest_dir = Path(args.download_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    minio_client = None
    if not args.skip_upload:
        minio_client = get_minio_client()
        ensure_bucket(minio_client, MINIO_BUCKET)

    # ---- Fetch & download ----
    downloaded: list[Path] = []

    if not args.skip_download:
        query   = build_query()
        papers  = fetch_papers(query)

        log.info("Starting downloads into: %s", dest_dir.resolve())
        for paper in tqdm(papers, desc="Downloading PDFs", unit="paper"):
            local_path = download_paper(paper, dest_dir)
            if local_path:
                downloaded.append((local_path, paper))
            time.sleep(1)   # gentle rate-limiting
    else:
        # Collect pre-existing files (no metadata available)
        downloaded = [(p, None) for p in dest_dir.glob("*.pdf")]
        log.info("Skip-download mode: found %d PDFs in %s", len(downloaded), dest_dir)

    # ---- Upload ----
    if args.skip_upload:
        log.info("Skip-upload mode: done.")
        return

    success = failed = 0
    for item in tqdm(downloaded, desc="Uploading to MinIO", unit="file"):
        local_path, paper = item if isinstance(item, tuple) else (item, None)

        # Minimal stub when paper metadata is unavailable
        if paper is None:
            class _Stub:
                def get_short_id(self): return local_path.stem
                title = local_path.stem
                published = None
                primary_category = "unknown"
            paper = _Stub()

        if upload_to_minio(minio_client, local_path, paper):
            success += 1
        else:
            failed += 1

    log.info("Upload complete — success: %d, failed: %d", success, failed)


if __name__ == "__main__":
    main()