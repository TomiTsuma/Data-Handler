#!/usr/bin/env python3
"""
Script to collect research papers from arXiv regarding
neural network activations.

This script searches arXiv for papers discussing activation functions
and related topics, downloads the PDFs, and uploads them to MinIO.
"""

import arxiv
import os
import time
import json
from pathlib import Path

# Configuration
OUTPUT_ROOT = "arxiv_papers"
MINIO_BUCKET = "activation_analysis"
MAX_RESULTS_PER_QUERY = 100
DELAY_BETWEEN_QUERIES = 2  # Seconds to avoid rate limits

# Queries for neural network activation functions
# These cover various activation function types and related research
QUERIES = {
    "relu_variants": 'all:("ReLU" OR "rectified linear unit") AND all:("neural network" OR "deep learning")',
    "sigmoid_tanh": 'all:("sigmoid" OR "tanh" OR "hyperbolic tangent") AND all:("activation function" OR "neural network")',
    "leaky_relu": 'all:("LeakyReLU" OR "leaky rectified linear unit")',
    "swish_mish": 'all:("Swish" OR "Mish" OR "GELU") AND all:("activation function")',
    "activation_comparison": 'all:("activation function") AND all:("comparison" OR "benchmark" OR "survey")',
    "activation_theory": 'all:("activation function") AND all:("theory" OR "derivation" OR "analysis")',
    "modern_activations": 'all:("activation function") AND all:("neural network" OR "deep learning")',
}


def collect_papers():
    """Fetch and download papers based on predefined queries."""
    root_path = Path(OUTPUT_ROOT)
    root_path.mkdir(parents=True, exist_ok=True)

    all_metadata = []

    print(f"Starting arXiv collection for neural network activations")
    print(f"Output directory: {root_path.resolve()}")
    print(f"MinIO bucket: {MINIO_BUCKET}")
    print("=" * 70)

    client = arxiv.Client()

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
            for result in client.results(search):
                # Generate a filename based on the paper ID
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

    # Save all metadata to a JSON file for easy reference
    metadata_file = root_path / "metadata.json"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(all_metadata, f, indent=4, ensure_ascii=False)

    print("=" * 70)
    print(f"Collection complete. Total papers: {len(all_metadata)}")
    print(f"Metadata saved to: {metadata_file.resolve()}")

    return all_metadata


if __name__ == "__main__":
    collect_papers()
