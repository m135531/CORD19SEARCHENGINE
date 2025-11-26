#!/usr/bin/env python3
"""
CORD-19 Forward Index Builder

Builds a forward index that stores a list of token IDs against each document.
The forward index format (little-endian binary):
    uint32 doc_count
    repeated per doc:
        uint32 doc_id
        uint32 token_count
        repeated uint32 token_id

This module reuses functions from lexicon.py:
    - iter_source_files: Iterate through dataset files
    - process_document: Extract text from JSON documents
    - tokenize: Tokenize and filter stopwords
    - Lexicon: Map tokens to integer IDs
    - load_stopwords: Load stopword list

The forward index is used as an intermediate step to build the inverted index.
"""

import struct
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from . import lexicon  # reuse iter_source_files, process_document, tokenize, Lexicon, load_stopwords


def _configure_dataset(input_dir: Path) -> None:
    """Point lexicon.py's globals at a different dataset root."""
    input_dir = input_dir.resolve()
    lexicon.ROOT_DIR = input_dir
    lexicon.PMC_DIR = input_dir / "pmc_json"
    lexicon.PDF_DIR = input_dir / "pdf_json"


@contextmanager
def _temporary_dataset(input_dir: Path):
    """Temporarily redirect lexicon's dataset globals to the supplied directory."""
    original_root = lexicon.ROOT_DIR
    original_pdf = lexicon.PDF_DIR
    original_pmc = lexicon.PMC_DIR
    _configure_dataset(input_dir)
    try:
        yield
    finally:
        lexicon.ROOT_DIR = original_root
        lexicon.PDF_DIR = original_pdf
        lexicon.PMC_DIR = original_pmc


def _serialize_forward_index(records: Sequence[Tuple[int, List[int]]], output_path: Path) -> None:
    """
    Persist the forward index to `output_path` using a compact binary format.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as f:
        f.write(struct.pack("<I", len(records)))
        for doc_id, token_ids in records:
            f.write(struct.pack("<II", doc_id, len(token_ids)))
            if token_ids:
                f.write(struct.pack(f"<{len(token_ids)}I", *token_ids))


def _write_doc_metadata(metadata: Sequence[Tuple[int, str]], output_path: Path) -> None:
    """Write a simple tab-separated mapping of doc_id to paper_id for debugging."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for doc_id, paper_id in metadata:
            handle.write(f"{doc_id}\t{paper_id}\n")


def build_forward_index(input_dir: Path, output_dir: Path, limit: Optional[int] = None) -> Dict[str, int]:
    """
    Build a forward index that maps document IDs to lists of token IDs.

    The forward index stores: {doc_id: [token_id1, token_id2, ...]}
    This is saved as a binary file for efficient storage and retrieval.

    Args:
        input_dir: Path to the dataset root that contains `pdf_json` / `pmc_json`.
        output_dir: Directory where forward_index.bin (and lexicon.bin) will be written.
        limit: Optional maximum number of documents to index (for testing).

    Returns:
        Dictionary with statistics:
            - documents_indexed: Number of documents processed
            - unique_terms: Number of unique tokens in lexicon
            - avg_doc_length: Average number of tokens per document
            - total_tokens: Total tokens across all documents

    Raises:
        FileNotFoundError: If input_dir does not exist.
        RuntimeError: If no documents were indexed.
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"Dataset directory not found: {input_dir}")

    forward_records: List[Tuple[int, List[int]]] = []
    doc_metadata: List[Tuple[int, str]] = []

    with _temporary_dataset(input_dir):
        # Initialize components
        stopwords = lexicon.load_stopwords()
        lex = lexicon.Lexicon()

        doc_iter = lexicon.iter_source_files()
        doc_count = 0
        total_tokens = 0
        docs_skipped = 0

        print(f"[ForwardIndex] Starting index build from {input_dir}")
        print(f"[ForwardIndex] Limit: {'all documents' if limit is None else limit}")

        # Process each document
        for _source_tag, json_path in doc_iter:
            try:
                doc = lexicon.process_document(json_path)
                tokens = lexicon.tokenize(doc["text"], stopwords)

                # Skip documents with no tokens
                if not tokens:
                    docs_skipped += 1
                    continue

                doc_id = doc_count
                token_ids = [lex.get_id(token) for token in tokens]
                forward_records.append((doc_id, token_ids))
                doc_metadata.append((doc_id, doc["paper_id"]))
                total_tokens += len(token_ids)
                doc_count += 1

                # Progress logging
                if doc_count % lexicon.LOG_EVERY == 0:
                    avg_tokens = total_tokens // doc_count if doc_count else 0
                    print(
                        f"[ForwardIndex] Processed {doc_count} docs, vocab={len(lex.id2word)}, avg_tokens={avg_tokens}"
                    )

                # Check limit
                if limit is not None and doc_count >= limit:
                    break
            except Exception as e:
                print(f"[ForwardIndex] Warning: Error processing {json_path.name}: {e}")
                docs_skipped += 1
                continue

    # Validate results
    if not forward_records:
        raise RuntimeError("No documents were indexed; forward index would be empty. "
                         f"Check that {input_dir} contains valid JSON files.")

    # Write output files
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    forward_path = output_dir / "forward_index.bin"
    lexicon_path = output_dir / "lexicon.bin"
    
    _serialize_forward_index(forward_records, forward_path)
    lex.write_binary(lexicon_path)
    _write_doc_metadata(doc_metadata, output_dir / "doc_ids.tsv")

    # Calculate and print statistics
    avg_len = total_tokens / doc_count if doc_count > 0 else 0.0
    
    stats = {
        "documents_indexed": doc_count,
        "unique_terms": len(lex.id2word),
        "avg_doc_length": avg_len,
        "total_tokens": total_tokens,
        "docs_skipped": docs_skipped,
        "doc_ids_recorded": len(doc_metadata),
    }

    print(f"\n{'='*60}")
    print(f"[ForwardIndex] Build Complete!")
    print(f"  Documents indexed: {doc_count}")
    print(f"  Documents skipped: {docs_skipped}")
    print(f"  Unique terms (vocab size): {len(lex.id2word)}")
    print(f"  Total tokens: {total_tokens:,}")
    print(f"  Average tokens per document: {avg_len:.2f}")
    print(f"  Forward index saved to: {forward_path}")
    print(f"  Lexicon saved to: {lexicon_path}")
    print(f"{'='*60}")

    return stats


if __name__ == "__main__":
    """
    Standalone execution: Build forward index using default paths from lexicon.py
    """
    default_root = lexicon.ROOT_DIR  # use existing defaults unless overridden
    default_output = Path(lexicon.OUTPUT_DIR)
    
    try:
        stats = build_forward_index(default_root, default_output, limit=None)
        print(f"\n✓ Forward index build successful!")
    except FileNotFoundError as e:
        print(f"\n✗ Error: {e}")
        print("  Please ensure the dataset directory exists and contains pdf_json/pmc_json folders.")
    except RuntimeError as e:
        print(f"\n✗ Error: {e}")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        raise