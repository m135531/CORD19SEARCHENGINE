#!/usr/bin/env python3
"""Tests for forward_index.py module"""

import shutil
import struct
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

try:
    from source.indexer.forward_index import build_forward_index, _serialize_forward_index
except ModuleNotFoundError:  # pragma: no cover - fallback for direct test runs
    sys.path.append(str(PROJECT_ROOT))
    from source.indexer.forward_index import build_forward_index, _serialize_forward_index


@pytest.fixture
def test_dataset(tmp_path):
    """Setup test dataset from test_data directory."""
    dataset_src = PROJECT_ROOT / "test_data"
    if not dataset_src.exists():
        pytest.skip("Test data directory not found")
    dataset_copy = tmp_path / "dataset"
    shutil.copytree(dataset_src, dataset_copy)
    return dataset_copy


def test_build_forward_index_creates_binary(test_dataset, tmp_path):
    """Test that build_forward_index creates the expected binary files."""
    output_dir = tmp_path / "output"
    stats = build_forward_index(test_dataset, output_dir, limit=10)

    forward_path = output_dir / "forward_index.bin"
    lexicon_path = output_dir / "lexicon.bin"
    doc_ids_path = output_dir / "doc_ids.tsv"

    assert forward_path.exists()
    assert lexicon_path.exists()
    assert doc_ids_path.exists()
    assert struct.unpack("<I", forward_path.read_bytes()[:4])[0] == 10
    assert stats["documents_indexed"] == 10
    assert stats["unique_terms"] > 0
    assert stats["doc_ids_recorded"] == 10
    assert len(doc_ids_path.read_text().splitlines()) == 10


def test_build_forward_index_with_limit(test_dataset, tmp_path):
    """Test that limit parameter works correctly."""
    output_dir = tmp_path / "output"
    stats = build_forward_index(test_dataset, output_dir, limit=5)

    forward_path = output_dir / "forward_index.bin"
    assert struct.unpack("<I", forward_path.read_bytes()[:4])[0] == 5
    assert stats["documents_indexed"] == 5


def test_forward_index_binary_format(test_dataset, tmp_path):
    """Test that the binary format is correct."""
    output_dir = tmp_path / "output"
    build_forward_index(test_dataset, output_dir, limit=3)

    data = (output_dir / "forward_index.bin").read_bytes()
    offset = 4  # Skip doc_count
    doc_count = struct.unpack_from("<I", data, 0)[0]

    assert doc_count == 3
    for doc_idx in range(doc_count):
        doc_id, token_count = struct.unpack_from("<II", data, offset)
        offset += 8
        assert doc_id == doc_idx
        if token_count > 0:
            token_ids = list(struct.unpack_from(f"<{token_count}I", data, offset))
            offset += 4 * token_count
            assert all(tid >= 0 for tid in token_ids)


def test_forward_index_error_handling(tmp_path):
    """Test error handling for missing dataset."""
    with pytest.raises(FileNotFoundError):
        build_forward_index(tmp_path / "nonexistent", tmp_path / "output")


def test_forward_index_empty_dataset(tmp_path):
    """Test handling of empty dataset."""
    empty_dataset = tmp_path / "empty_dataset"
    (empty_dataset / "pdf_json").mkdir(parents=True)
    (empty_dataset / "pmc_json").mkdir()

    with pytest.raises(RuntimeError, match="No documents were indexed"):
        build_forward_index(empty_dataset, tmp_path / "output")


def test_serialize_forward_index(tmp_path):
    """Test the serialization function directly."""
    records = [(0, [1, 2, 3]), (1, [2, 3, 4, 5]), (2, [1, 5])]
    output_path = tmp_path / "test_forward.bin"
    _serialize_forward_index(records, output_path)

    data = output_path.read_bytes()
    assert struct.unpack("<I", data[:4])[0] == 3

    offset = 4
    for doc_id, token_ids in records:
        read_doc_id, token_count = struct.unpack_from("<II", data, offset)
        offset += 8
        assert read_doc_id == doc_id
        if token_count > 0:
            read_tokens = list(struct.unpack_from(f"<{token_count}I", data, offset))
            offset += 4 * token_count
            assert read_tokens == token_ids
