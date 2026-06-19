#!/usr/bin/env python3
"""Debug script to test Arxiv query."""

import requests
import feedparser

query = "de novo diffusion"
base_url = "https://export.arxiv.org/api/query?search_query={}&start={}&max_results={}"

url = base_url.format(query, 0, 20)
print(f"Query URL: {url}")
print()

response = requests.get(url)
feed = feedparser.parse(response.content)

print(f"Total results: {feed.feed.get('opensearch:totalResults', 'N/A')}")
print(f"Entries returned: {len(feed.entries)}")
print()

for i, entry in enumerate(feed.entries[:5]):
    print(f"Entry {i+1}:")
    print(f"  Title: {entry.title}")
    print(f"  Summary: {entry.summary[:200]}...")
    print(f"  ID: {entry.id}")
    print()
