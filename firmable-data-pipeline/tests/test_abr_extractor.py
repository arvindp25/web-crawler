import pytest
from extract.abr_extractor import extract_abr_records


def test_extract_abr_records_limited_files():
    max_files = 2
    records = list(extract_abr_records(max_files=max_files))

    # Basic checks
    assert isinstance(records, list)
    assert len(records) > 0, "Expected records from ABR extractor."

    for rec in records:
        assert "abn" in rec
        assert "entity_name" in rec
        assert "state" in rec
        assert rec["abn"] is not None