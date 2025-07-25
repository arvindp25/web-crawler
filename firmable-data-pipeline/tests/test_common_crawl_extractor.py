# tests/test_common_crawl_extractor.py

import pytest
from extract.common_crawl_extractor import search_common_crawl

def test_common_crawl_search_returns_valid_records():
    """
    System test: Ensure Common Crawl search returns valid structured records.
    """
    generator = search_common_crawl("com.au", pages=1)
    record = next(generator)

    assert isinstance(record, dict), "Returned record is not a dictionary"
    assert "url" in record
    assert "timestamp" in record
    assert "mime" in record
    assert "status" in record
    assert record["url"].endswith(".au") or ".au" in record["url"]
    assert record["status"] == "200"
