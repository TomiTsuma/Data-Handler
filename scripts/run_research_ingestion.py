#!/usr/bin/env python3
"""
Research Paper Ingestion Pipeline

Downloads research papers on:
- Graph Neural Networks (GNN) in Drug Discovery and Medicine
- Transformers in Drug Discovery and Medicine
- Reinforcement Learning (RL) in Drug Discovery and Medicine

Also supports hybrid approaches combining these techniques.
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

# Research topics and their arXiv query strings
# Using arXiv API query syntax with all: for full-text search
RESEARCH_TOPICS = {
    # "gnn-drug-discovery": {  # SKIPPED - too many results
    #     "name": "gnn-drug-discovery",
    #     "query": 'all:("graph neural network" OR "graph convolutional" OR "GNN") AND ("drug discovery" OR "pharmaceutical" OR "molecular" OR "compound" OR "molecule")',
    #     "bucket": "research-papers",
    #     "description": "Graph Neural Networks for molecular property prediction, drug discovery, compound generation"
    # },
    "gnn-medicine": {
        "name": "gnn-medicine",
        "query": 'all:("graph neural network" OR "graph convolutional" OR "GNN") AND ("medical" OR "clinical" OR "disease" OR "biomarker" OR "patient" OR "diagnosis")',
        "bucket": "research-papers",
        "description": "Graph Neural Networks in medical diagnosis, disease prediction, clinical data analysis"
    },
    "transformers-drug-discovery": {
        "name": "transformers-drug-discovery",
        "query": 'all:("transformer" OR "BERT" OR "attention mechanism") AND ("drug discovery" OR "molecular property" OR "compound" OR "SMILES" OR "protein sequence")',
        "bucket": "research-papers",
        "description": "Transformer models for molecular representation, protein sequence analysis, drug-property prediction"
    },
    "transformers-medicine": {
        "name": "transformers-medicine",
        "query": 'all:("transformer" OR "BERT" OR "clinical transformer") AND ("medical text" OR "EHR" OR "clinical notes" OR "medical imaging" OR "diagnosis")',
        "bucket": "research-papers",
        "description": "Transformers for medical NLP, EHR analysis, clinical decision support, medical imaging"
    },
    "rl-drug-discovery": {
        "name": "rl-drug-discovery",
        "query": 'all:("reinforcement learning" OR "deep reinforcement learning" OR "RL") AND ("drug discovery" OR "molecular design" OR "de novo" OR "generative model" OR "molecular optimization")',
        "bucket": "research-papers",
        "description": "RL for molecular generation, drug repurposing, treatment optimization"
    },
    "rl-precision-medicine": {
        "name": "rl-precision-medicine",
        "query": 'all:("reinforcement learning" OR "RL") AND ("precision medicine" OR "personalized treatment" OR "dosing" OR "therapy optimization" OR "adaptive treatment")',
        "bucket": "research-papers",
        "description": "RL for personalized treatment strategies, adaptive dosing, clinical decision support"
    },
    "gnn-rl-hybrid": {
        "name": "gnn-rl-hybrid",
        "query": 'all:(("graph neural network" OR "GNN") AND ("reinforcement learning" OR "RL")) AND ("drug" OR "molecule" OR "compound")',
        "bucket": "research-papers",
        "description": "Hybrid GNN+RL architectures for molecular generation and optimization"
    },
    "gnn-massive-activation": {
        "name": "gnn-massive-activation",
        "query": 'all:("massive activation" OR "activation patterns" OR "activation maximization") AND ("graph neural network" OR "graph convolutional" OR "GNN")',
        "bucket": "research-papers",
        "description": "Massive activation analysis in graph neural networks, activation patterns, interpretability"
    },
    "all": {
        "name": "all",
        "query": "*",
        "bucket": "research-papers",
        "description": "Download papers from all research topic categories"
    }
}


def run_ingestion(topic: dict, max_results: int = 100, max_pages: int = 20, dry_run: bool = False) -> bool:
    """Run a single ingestion job for a research topic."""
    cmd = [
        "python3",
        str(Path(__file__).parent / "run_ingestion.py"),
        "--source", "arxiv",
        "--dataset-id", topic["name"],
        "--bucket", topic["bucket"],
        "--arxiv-category", topic["query"],
        "--query-mode", "query",
        "--workspace", "data/tmp",
        "--max-results", str(max_results),
        "--max-pages", str(max_pages),
    ]

    if dry_run:
        print(f"\n{'='*60}")
        print(f"DRY RUN - Starting: {topic['description']}")
        print(f"Query: {topic['query']}")
        print(f"Max results: {max_results}, Max pages: {max_pages}")
        print(f"{'='*60}\n")
        return True

    print(f"\n{'='*60}")
    print(f"Starting: {topic['description']}")
    print(f"Query: {topic['query']}")
    print(f"Max results: {max_results}, Max pages: {max_pages}")
    print(f"{'='*60}\n")

    result = subprocess.run(cmd, capture_output=False)
    return result.returncode == 0


def run_all_topics(max_results: int = 100, max_pages: int = 20, dry_run: bool = False) -> bool:
    """Run ingestion for all research topics."""
    print("="*60)
    print("RESEARCH PAPER INGESTION PIPELINE")
    print("Topics: GNNs, Transformers, RL in Drug Discovery/Medicine")
    print(f"Max results per topic: {max_results}, Max pages per topic: {max_pages}")
    print("="*60)

    results = []
    for topic in RESEARCH_TOPICS.values():
        success = run_ingestion(topic, max_results=max_results, max_pages=max_pages, dry_run=dry_run)
        results.append({
            "topic": topic["name"],
            "description": topic["description"],
            "success": success
        })

    print("\n" + "="*60)
    print("INGESTION SUMMARY")
    print("="*60)
    for r in results:
        status = "✓ SUCCESS" if r["success"] else "✗ FAILED"
        print(f"  {status}: {r['topic']} - {r['description']}")

    failed = sum(1 for r in results if not r["success"])
    if failed > 0:
        print(f"\n{failed} job(s) failed. Check logs for details.")
        return False
    else:
        print("\nAll ingestion jobs completed successfully!")
        return True


def run_custom_query(query: str, bucket: str, max_results: int = 100, max_pages: int = 20, dry_run: bool = False) -> bool:
    """Run ingestion for a custom arXiv query."""
    print("="*60)
    print("CUSTOM RESEARCH PAPER INGESTION")
    print(f"Query: {query}")
    print(f"Max results: {max_results}, Max pages: {max_pages}")
    print("="*60)

    if dry_run:
        print(f"\nDRY RUN - Would download papers matching query")
        return True

    # Create a temporary topic dict for the custom query
    topic = {
        "name": f"custom-{query[:20].replace(' ', '-').replace(':', '')}",
        "query": query,
        "bucket": bucket,
        "description": f"Custom query: {query[:50]}..."
    }

    success = run_ingestion(topic, max_results=max_results, max_pages=max_pages)
    return success


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download research papers on GNN, RL, and Transformers for Drug Discovery and Medicine"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--topic",
        choices=list(RESEARCH_TOPICS.keys()),
        default="all",
        help="Research topic to download papers for (default: all)",
    )
    group.add_argument(
        "--custom-query",
        type=str,
        help="Custom arXiv search query (use arXiv query syntax)",
    )

    parser.add_argument(
        "--max-results",
        type=int,
        default=100,
        help="Maximum number of papers to download (default: 100)",
    )
    parser.add_argument(
        "--bucket",
        help="MinIO bucket to use (defaults to MINIO_DEFAULT_BUCKET)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=20,
        help="Maximum number of pages to download per topic (default: 20)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be downloaded without actually downloading",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Print topic info
    if args.topic != "custom-query":
        topic_info = RESEARCH_TOPICS[args.topic]
        print("="*60)
        print(f"Research Paper Ingestion: {topic_info['name']}")
        print("="*60)
        print(f"Description: {topic_info['description']}")
        print(f"Query: {topic_info['query']}")
        print(f"Max results: {args.max_results}, Max pages: {args.max_pages}")
        print("="*60)

    if args.topic == "all":
        success = run_all_topics(max_results=args.max_results, max_pages=args.max_pages, dry_run=args.dry_run)
    elif args.topic == "custom-query":
        if not args.custom_query:
            print("Error: --custom-query requires a query string")
            sys.exit(1)
        bucket = args.bucket or "research-papers"
        success = run_custom_query(args.custom_query, bucket, max_results=args.max_results, max_pages=args.max_pages, dry_run=args.dry_run)
    else:
        topic = RESEARCH_TOPICS[args.topic]
        success = run_ingestion(topic, max_results=args.max_results, max_pages=args.max_pages, dry_run=args.dry_run)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()