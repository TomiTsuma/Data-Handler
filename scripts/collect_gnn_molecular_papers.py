#!/usr/bin/env python3
"""
Script to collect research papers from arXiv regarding
Graph Neural Networks for Energetic Molecular Property Prediction.
"""

import arxiv
import os
import time
import json
from pathlib import Path

# Configuration
OUTPUT_ROOT = "gnn_molecular_papers"
MAX_RESULTS_PER_QUERY = 50
DELAY_BETWEEN_QUERIES = 3  # Seconds to avoid rate limits

# Defined queries for GNNs and Molecular Property Prediction
QUERIES = {
    "gnn_molecular_prediction": 'all:("Graph Neural Networks" OR "GNN") AND all:("molecular property prediction" OR "energetic materials")',
    "molecular_property_dl": 'all:("deep learning" OR "machine learning") AND all:("molecular property prediction")',
    "energetic_gnn": 'all:("GNN" OR "Graph Neural Network") AND all:("energetic materials")',
}

def collect_papers():
    """Fetch and download papers based on predefined queries."""
    root_path = Path(OUTPUT_ROOT)
    root_path.mkdir(parents=True, exist_ok=True)

    all_metadata = []

    print(f"Starting arXiv collection into: {root_path.resolve()}")
    print("=" * 60)

    client = arxiv.Client()

    for topic, query in QUERIES.items():
        print(f"Fetching papers for topic: {topic}...")
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
                    print(f"  Downloading: {result.title[:60]}...")
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

            print(f"Successfully collected {count} papers for {topic}.\n")
        except Exception as e:
            print(f"Error fetching papers for {topic}: {e}")

        time.sleep(DELAY_BETWEEN_QUERIES)

    # Save all metadata to a JSON file for easy reference
    metadata_file = root_path / "metadata.json"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(all_metadata, f, indent=4, ensure_ascii=False)

    print("=" * 60)
    print(f"Collection complete. Total papers: {len(all_metadata)}")
    print(f"Metadata saved to: {metadata_file.resolve()}")

if __name__ == "__main__":
    collect_papers()
