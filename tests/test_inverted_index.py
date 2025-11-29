#!/usr/bin/env python3
"""Tests for inverted_index.py module"""

import shutil
import struct
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from source.indexer import lexicon
from source.indexer.forward_index import build_forward_index
from source.indexer.inverted_index import (
    build_inverted_index,
    _load_forward_index,
    _serialize_inverted_index,
)


@pytest.fixture
def test_dataset(tmp_path):
    """Setup test dataset from test_data directory."""
    dataset_src = PROJECT_ROOT / "test_data"
    if not dataset_src.exists():
        pytest.skip("Test data directory not found")
    dataset_copy = tmp_path / "dataset"
    shutil.copytree(dataset_src, dataset_copy)
    return dataset_copy


@pytest.fixture
def lexicon_context():
    """Save and restore lexicon paths."""
    original_root = lexicon.ROOT_DIR
    original_pdf = lexicon.PDF_DIR
    original_pmc = lexicon.PMC_DIR
    yield
    lexicon.ROOT_DIR = original_root
    lexicon.PDF_DIR = original_pdf
    lexicon.PMC_DIR = original_pmc


def create_forward_index(path: Path, records: list):
    """Helper to create a forward index file."""
    with path.open("wb") as f:
        f.write(struct.pack("<I", len(records)))
        for doc_id, token_ids in records:
            f.write(struct.pack("<II", doc_id, len(token_ids)))
            if token_ids:
                f.write(struct.pack(f"<{len(token_ids)}I", *token_ids))


def test_build_inverted_index_from_forward(test_dataset, lexicon_context, tmp_path):
    """Test building inverted index from a forward index file."""
    output_dir = tmp_path / "output"
    build_forward_index(test_dataset, output_dir, limit=10)

    forward_path = output_dir / "forward_index.bin"
    stats = build_inverted_index(forward_path, output_dir)

    assert (output_dir / "inverted_index.bin").exists()
    assert stats["unique_tokens"] > 0
    assert stats["documents_indexed"] == 10


def test_inverted_index_binary_format(tmp_path):
    """Test that the inverted index binary format is correct."""
    forward_path = tmp_path / "forward_index.bin"
    create_forward_index(forward_path, [(0, [1, 2, 3]), (1, [2, 3, 4]), (2, [1, 4, 5])])

    build_inverted_index(forward_path, tmp_path)

    data = (tmp_path / "inverted_index.bin").read_bytes()
    vocab_size = struct.unpack_from("<I", data, 0)[0]
    assert vocab_size == 5

    expected = {1: [0, 2], 2: [0, 1], 3: [0, 1], 4: [1, 2], 5: [2]}
    offset = 4
    for _ in range(vocab_size):
        token_id, doc_freq = struct.unpack_from("<II", data, offset)
        offset += 8
        if doc_freq > 0:
            docs = list(struct.unpack_from(f"<{doc_freq}I", data, offset))
            offset += 4 * doc_freq
            assert docs == expected[token_id]


def test_inverted_index_deduplication(tmp_path):
    """Test that duplicate token occurrences are deduplicated."""
    forward_path = tmp_path / "forward_index.bin"
    create_forward_index(forward_path, [(0, [1, 1, 2, 2, 2, 3]), (1, [2, 3, 4])])

    build_inverted_index(forward_path, tmp_path)

    data = (tmp_path / "inverted_index.bin").read_bytes()
    postings = {}
    vocab_size = struct.unpack_from("<I", data, 0)[0]
    offset = 4

    for _ in range(vocab_size):
        token_id, doc_freq = struct.unpack_from("<II", data, offset)
        offset += 8
        if doc_freq > 0:
            docs = list(struct.unpack_from(f"<{doc_freq}I", data, offset))
            offset += 4 * doc_freq
            postings[token_id] = docs

    assert postings[1] == [0]  # Deduplicated
    assert postings[2] == [0, 1]  # Doc 0 appears once


def test_inverted_index_sorted_postings(tmp_path):
    """Test that postings are sorted by document ID."""
    forward_path = tmp_path / "forward_index.bin"
    create_forward_index(forward_path, [(5, [1, 2]), (2, [1, 3]), (8, [2, 3]), (1, [1, 2, 3])])

    build_inverted_index(forward_path, tmp_path)

    data = (tmp_path / "inverted_index.bin").read_bytes()
    vocab_size = struct.unpack_from("<I", data, 0)[0]
    offset = 4

    for _ in range(vocab_size):
        token_id, doc_freq = struct.unpack_from("<II", data, offset)
        offset += 8
        if doc_freq > 0:
            docs = list(struct.unpack_from(f"<{doc_freq}I", data, offset))
            offset += 4 * doc_freq
            assert docs == sorted(docs)


def test_inverted_index_error_handling(tmp_path):
    """Test error handling for missing forward index."""
    with pytest.raises(FileNotFoundError):
        build_inverted_index(tmp_path / "nonexistent.bin", tmp_path)


def test_inverted_index_empty_forward_index(tmp_path):
    """Test handling of empty forward index."""
    forward_path = tmp_path / "empty_forward_index.bin"
    with forward_path.open("wb") as f:
        f.write(struct.pack("<I", 0))

    with pytest.raises(RuntimeError, match="Forward index is empty"):
        build_inverted_index(forward_path, tmp_path)


def test_load_forward_index(tmp_path):
    """Test loading forward index from binary file."""
    records = [(0, [1, 2, 3]), (1, [2, 4]), (2, [1, 3, 5])]
    forward_path = tmp_path / "test_forward.bin"
    create_forward_index(forward_path, records)

    assert _load_forward_index(forward_path) == records


def test_serialize_inverted_index(tmp_path):
    """Test the serialization function directly."""
    postings = {1: [0, 2, 5], 2: [0, 1], 3: [0, 2], 4: [1], 5: [2]}
    output_path = tmp_path / "test_inverted.bin"
    _serialize_inverted_index(postings, output_path)

    data = output_path.read_bytes()
    assert struct.unpack("<I", data[:4])[0] == 5

    offset = 4
    for token_id in sorted(postings.keys()):
        read_token_id, doc_freq = struct.unpack_from("<II", data, offset)
        offset += 8
        assert read_token_id == token_id
        if doc_freq > 0:
            read_docs = list(struct.unpack_from(f"<{doc_freq}I", data, offset))
            offset += 4 * doc_freq
            assert read_docs == postings[token_id]
