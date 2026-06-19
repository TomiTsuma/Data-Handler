#!/usr/bin/env python3
"""Debug script to test different Arxiv query formats."""

import requests
import feedparser

def test_query(query, max_results=5):
    base_url = "https://export.arxiv.org/api/query?search_query={}&start={}&max_results={}"
    url = base_url.format(query, 0, max_results)
    response = requests.get(url)
    feed = feedparser.parse(response.content)
    total = feed.feed.get('opensearch:totalResults', 'N/A')
    print(f"Query: '{query}'")
    print(f"  Total results: {total}")
    print(f"  Entries returned: {len(feed.entries)}")
    for i, entry in enumerate(feed.entries[:3]):
        print(f"    {i+1}. {entry.title[:80]}")
    print()

# Test different query formats
test_query("de novo diffusion", 5)
test_query("diffusion", 5)
test_query("de novo", 5)
test_query("ti:de novo", 5)
test_query("ti:diffusion", 5)
test_query("ti:(de novo OR diffusion)", 5)
test_query("abs:(de novo diffusion)", 5)
test_query("all:(de novo diffusion)", 5)
