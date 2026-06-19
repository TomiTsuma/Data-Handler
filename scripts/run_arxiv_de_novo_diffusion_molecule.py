#!/usr/bin/env python3
"""
Run arxiv query for: de novo diffusion molecule papers
Query parameters:
- order: -announced_date_first (most recent first)
- size: 50
- include_older_versions: True
- date_range: from 2022-01-01 to 2026-12-31
- classification: Computer Science (cs)
- include_cross_list: True
- terms: AND all="de novo"; AND all="diffusion"; AND all="molecule"
"""

import arxiv
from datetime import datetime
import time
import os
import shutil
from pathlib import Path

def run_arxiv_query():
    """Run the arxiv query for de novo diffusion molecule papers."""

    # Build the query string with all required terms
    # Using all:() to search across all fields
    query = "all:(de novo)+AND+all:(diffusion)+AND+all:(molecule)"

    # Date range filter (2022-01-01 to 2026-12-31)
    # arxiv uses submittedDate:YMMDD format
    date_from = "20220101"
    date_to = "20261231"

    # Classification filter - Computer Science (cs)
    # This includes all cs subcategories
    cs_filter = "cat:cs"

    # Combine all filters
    full_query = f"({cs_filter})+AND+(all:(de+novo))+AND+(all:(diffusion))+AND+(all:(molecule))"

    print(f"Query: {full_query}")
    print(f"Date range: {date_from} to {date_to}")
    print()

    # Create timestamped subfolder for storage
    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
    base_dir = Path("/home/rhadamanthys/Data-Handler/data/tmp")
    storage_folder = base_dir / f"arxiv_{timestamp}"
    storage_folder.mkdir(parents=True, exist_ok=True)
    print(f"Storage folder: {storage_folder}")

    client = arxiv.Client()

    search = arxiv.Search(
        query=full_query,
        max_results=50,
        sort_by=arxiv.SortCriterion.SubmittedDate,
        sort_order=arxiv.SortOrder.Descending
    )

    print("Fetching results...")
    papers = []
    date_from_dt = datetime(2022, 1, 1)
    date_to_dt = datetime(2026, 12, 31)

    for result in client.results(search):
        if len(papers) >= 50:
            break

        # Filter by date range
        result_date = result.updated
        # Make timezone-naive for comparison
        if hasattr(result_date, 'tzinfo') and result_date.tzinfo is not None:
            result_date = result_date.replace(tzinfo=None)
        if isinstance(result_date, datetime):
            result_date = result_date.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            result_date = datetime.strptime(result_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)

        if result_date < date_from_dt or result_date > date_to_dt:
            continue

        papers.append(result)

        # Print paper info
        print(f"\n{len(papers)}. {result.title}")
        print(f"   ID: {result.entry_id}")
        print(f"   Submitted: {result.updated.date()}")
        print(f"   Categories: {', '.join(result.categories[:5])}")
        print(f"   Abstract: {result.summary[:200]}...")

        # Download PDF
        try:
            pdf_filename = f"{result.title[:50].replace(' ', '_').replace('/', '_')}.pdf"
            pdf_path = storage_folder / pdf_filename
            result.download_pdf(dirpath=str(storage_folder), filename=pdf_filename)
            print(f"   Downloaded: {pdf_filename}")
        except Exception as e:
            print(f"   Download failed: {e}")

        time.sleep(1)  # Respect arXiv rate limits

    print(f"\n\nTotal papers found: {len(papers)}")
    print(f"Query executed at: {datetime.now().isoformat()}")
    print(f"Papers stored in: {storage_folder}")

    return papers

if __name__ == "__main__":
    papers = run_arxiv_query()
