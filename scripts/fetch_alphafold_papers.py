#!/usr/bin/env python3
"""Fetch research papers about AlphaFold from arXiv."""

from pathlib import Path
import requests
import feedparser
import time
import os

OUTPUT_DIR = "alphafold_papers"
os.makedirs(OUTPUT_DIR, exist_ok=True)

BASE_URL_QUERY = "http://export.arxiv.org/api/query?search_query={}&start={}&max_results={}"

def download_pdf(entry):
    """Download PDF from arXiv entry."""
    pdf_url = entry.id.replace('abs', 'pdf') + ".pdf"
    response = requests.get(pdf_url)
    if response.status_code == 200:
        file_path = Path(OUTPUT_DIR) / f"{entry.id.split('/')[-1]}.pdf"
        with open(file_path, 'wb') as f:
            f.write(response.content)
        return file_path
    return None

def fetch_alphafold_papers(max_results=20):
    """Fetch AlphaFold papers from arXiv."""
    # Search for papers with AlphaFold in title or abstract
    query = "ti:AlphaFold OR ti:alphafold OR abs:AlphaFold OR abs:alphafold"
    file_paths = []

    for start in range(0, max_results, 10):
        url = BASE_URL_QUERY.format(query, start, 10)
        response = requests.get(url)
        feed = feedparser.parse(response.content)

        for entry in feed.entries:
            file_path = download_pdf(entry)
            if file_path:
                file_paths.append((file_path, entry))
                print(f"Downloaded: {file_path.name}")
                print(f"  Title: {entry.title}")
                print(f"  Authors: {', '.join(entry.author.split()[:3])}...")
                print(f"  Published: {entry.published[:10]}")
                print()

        if len(feed.entries) < 10:
            break
        time.sleep(3)  # Respect arXiv rate limits

    return file_paths

if __name__ == "__main__":
    print("Fetching AlphaFold research papers from arXiv...")
    print("=" * 60)
    papers = fetch_alphafold_papers(max_results=20)
    print(f"\nTotal papers downloaded: {len(papers)}")
    print(f"Saved to: {Path(OUTPUT_DIR).resolve()}")
