#!/usr/bin/env python3
"""
Script to collect research papers from arXiv regarding neural network activations
and push them to MinIO.

This script:
1. Searches arXiv for papers about activation functions
2. Downloads the PDFs
3. Creates the MinIO bucket "activation_analysis" if it doesn't exist
4. Uploads all papers to the bucket
"""

import arxiv
import os
import time
import json
from pathlib import Path

# Load environment variables
from dotenv import load_dotenv
load_dotenv("/home/rhadamanthys/Data-Handler/.env")

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "100.127.65.29:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password")
MINIO_SECURE = os.getenv("MINIO_USE_SSL", "false").lower() in ("true", "1", "yes")

# ArXiv Configuration
OUTPUT_ROOT = "arxiv_papers"
MINIO_BUCKET = "activation_analysis"
MAX_RESULTS_PER_QUERY = 100
DELAY_BETWEEN_QUERIES = 2

# Queries for neural network activation functions
QUERIES = {
    "relu_variants": 'all:("ReLU" OR "rectified linear unit") AND all:("neural network" OR "deep learning")',
    "sigmoid_tanh": 'all:("sigmoid" OR "tanh" OR "hyperbolic tangent") AND all:("activation function" OR "neural network")',
    "leaky_relu": 'all:("LeakyReLU" OR "leaky rectified linear unit")',
    "swish_mish": 'all:("Swish" OR "Mish" OR "GELU") AND all:("activation function")',
    "activation_comparison": 'all:("activation function") AND all:("comparison" OR "benchmark" OR "survey")',
    "activation_theory": 'all:("activation function") AND all:("theory" OR "derivation" OR "analysis")',
    "modern_activations": 'all:("activation function") AND all:("neural network" OR "deep learning")',
}


def get_minio_client():
    """Create and return a MinIO client instance."""
    from minio import Minio
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client, bucket_name):
    """Create bucket if it doesn't exist."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created MinIO bucket: {bucket_name}")
        return True
    print(f"Bucket already exists: {bucket_name}")
    return False


def upload_file(client, bucket, source_path, object_name):
    """Upload a single file to MinIO with retry logic."""
    from minio.error import S3Error
    import time

    source_path = Path(source_path)
    if not source_path.exists():
        print(f"Error: File not found: {source_path}")
        return False

    last_exception = None

    for attempt in range(3):
        try:
            client.fput_object(
                bucket,
                object_name,
                str(source_path)
            )
            print(f"  Uploaded: {source_path.name} -> s3://{bucket}/{object_name}")
            return True
        except S3Error as exc:
            last_exception = exc
            print(f"  Upload attempt {attempt + 1} failed: {exc}")
            if attempt < 2:
                time.sleep(2 ** attempt)

    print(f"  Failed to upload {source_path} after 3 retries: {last_exception}")
    return False


def collect_and_upload_papers():
    """Fetch and download papers, then upload to MinIO."""
    root_path = Path(OUTPUT_ROOT)
    root_path.mkdir(parents=True, exist_ok=True)

    all_metadata = []
    uploaded_count = 0

    print(f"Starting arXiv collection for neural network activations")
    print(f"Output directory: {root_path.resolve()}")
    print(f"MinIO bucket: {MINIO_BUCKET}")
    print(f"MinIO endpoint: {MINIO_ENDPOINT}")
    print("=" * 70)

    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    arxiv_client = arxiv.Client()

    for topic, query in QUERIES.items():
        print(f"\nFetching papers for topic: {topic}...")
        print(f"Query: {query}")

        topic_dir = root_path / topic
        topic_dir.mkdir(exist_ok=True)

        search = arxiv.Search(
            query=query,
            max_results=MAX_RESULTS_PER_QUERY,
            sort_by=arxiv.SortCriterion.SubmittedDate
        )

        count = 0
        try:
            for result in arxiv_client.results(search):
                paper_id = result.entry_id.split('/')[-1]
                filename = f"{paper_id}.pdf"
                pdf_path = topic_dir / filename

                if not pdf_path.exists():
                    print(f"  Downloading: {result.title[:70]}...")
                    try:
                        result.download_pdf(dirpath=str(topic_dir), filename=filename)
                    except Exception as e:
                        print(f"    ✗ Failed to download {filename}: {e}")
                        continue
                else:
                    print(f"  Already exists: {filename}")

                # Collect metadata
                all_metadata.append({
                    "topic": topic,
                    "title": result.title,
                    "authors": [author.name for author in result.authors],
                    "published": result.published.strftime("%Y-%m-%d"),
                    "url": result.entry_id,
                    "pdf_local_path": str(pdf_path),
                    "summary": result.summary
                })
                count += 1

                # Rate limiting
                if count % 10 == 0:
                    time.sleep(DELAY_BETWEEN_QUERIES)

            print(f"Successfully collected {count} papers for {topic}.\n")
        except Exception as e:
            print(f"Error fetching papers for {topic}: {e}")

    # Upload all downloaded papers to MinIO
    print("\n" + "=" * 70)
    print("Uploading papers to MinIO...")
    print("=" * 70)

    for topic in QUERIES.keys():
        topic_dir = root_path / topic
        if topic_dir.exists():
            for pdf_path in topic_dir.iterdir():
                if pdf_path.is_file() and pdf_path.suffix == ".pdf":
                    # Create object name with topic prefix
                    object_name = f"{topic}/{pdf_path.name}"
                    if upload_file(client, MINIO_BUCKET, pdf_path, object_name):
                        uploaded_count += 1

    # Save metadata
    metadata_file = root_path / "metadata.json"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(all_metadata, f, indent=4, ensure_ascii=False)

    print("=" * 70)
    print(f"Collection and upload complete!")
    print(f"Total papers collected: {len(all_metadata)}")
    print(f"Total papers uploaded to MinIO: {uploaded_count}")
    print(f"Metadata saved to: {metadata_file.resolve()}")
    print(f"Papers in MinIO: s3://{MINIO_BUCKET}/")

    return all_metadata, uploaded_count


if __name__ == "__main__":
    collect_and_upload_papers()
