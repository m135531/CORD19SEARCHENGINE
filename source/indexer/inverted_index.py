#!/usr/bin/env python3
"""
CORD-19 Inverted Index Builder

Constructs an inverted index from a forward index file.
The inverted index stores: {token_id: [doc_id1, doc_id2, ...]}

Binary layout for inverted_index.bin (little-endian):
    uint32 vocab_size (number of unique tokens)
    repeated per token:
        uint32 token_id
        uint32 doc_freq (number of documents containing this token)
        repeated uint32 doc_id (sorted list of document IDs)

The inverted index is used during search to quickly identify which documents
contain specific words. Postings are sorted and deduplicated for efficiency.
"""

import struct
from pathlib import Path
from typing import Dict, List, Tuple


def _load_forward_index(path: Path) -> List[Tuple[int, List[int]]]:
    """
    Read forward_index.bin (little-endian) and return a list of (doc_id, token_ids).
    """
    with path.open("rb") as f:
        data = f.read()

    offset = 0
    doc_count = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    records: List[Tuple[int, List[int]]] = []

    for _ in range(doc_count):
        doc_id, token_count = struct.unpack_from("<II", data, offset)
        offset += 8
        tokens_fmt = f"<{token_count}I"
        token_ids = list(struct.unpack_from(tokens_fmt, data, offset)) if token_count else []
        offset += 4 * token_count
        records.append((doc_id, token_ids))

    return records


def _serialize_inverted_index(postings: Dict[int, List[int]], output_path: Path) -> None:
    """
    Persist the postings dictionary to disk in a compact binary format.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as f:
        f.write(struct.pack("<I", len(postings)))
        for token_id in sorted(postings.keys()):
            docs = postings[token_id]
            f.write(struct.pack("<II", token_id, len(docs)))
            if docs:
                f.write(struct.pack(f"<{len(docs)}I", *docs))


def build_inverted_index(forward_index_path: Path, output_dir: Path) -> Dict[str, int]:
    """
    Build an inverted index from the forward index file.

    The inverted index maps token IDs to lists of document IDs where each token appears.
    Postings are sorted and deduplicated for efficient search operations.

    Args:
        forward_index_path: Path to forward_index.bin produced by forward_index.py.
        output_dir: Directory where inverted_index.bin will be written.

    Returns:
        Dictionary with statistics:
            - unique_tokens: Number of unique tokens in the index
            - total_postings: Total number of (token, doc) pairs
            - avg_postings_per_token: Average number of documents per token
            - documents_indexed: Number of documents in the forward index

    Raises:
        FileNotFoundError: If forward_index_path does not exist.
        RuntimeError: If the forward index is empty or corrupted.
    """
    if not forward_index_path.exists():
        raise FileNotFoundError(f"Forward index not found: {forward_index_path}")

    print(f"[InvertedIndex] Loading forward index from {forward_index_path}")
    
    # Load forward index records
    try:
        records = _load_forward_index(forward_index_path)
    except Exception as e:
        raise RuntimeError(f"Failed to load forward index: {e}")

    if not records:
        raise RuntimeError("Forward index is empty; cannot build inverted index.")

    print(f"[InvertedIndex] Processing {len(records)} documents...")

    # Build inverted postings: token_id -> set of doc_ids
    postings: Dict[int, set] = {}
    total_token_occurrences = 0

    for idx, (doc_id, token_ids) in enumerate(records, start=1):
        for token_id in token_ids:
            postings.setdefault(token_id, set()).add(doc_id)
            total_token_occurrences += 1

        if idx % 50 == 0:
            print(f"[InvertedIndex] Processed {idx} documents")

    # Convert sets to sorted lists for deterministic storage and efficient merging
    sorted_postings: Dict[int, List[int]] = {
        token_id: sorted(doc_ids) for token_id, doc_ids in postings.items()
    }

    # Write inverted index to disk
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    inverted_path = output_dir / "inverted_index.bin"
    _serialize_inverted_index(sorted_postings, inverted_path)

    # Calculate statistics
    total_postings = sum(len(docs) for docs in sorted_postings.values())
    avg_postings = total_postings / len(sorted_postings) if sorted_postings else 0.0

    stats = {
        "unique_tokens": len(sorted_postings),
        "total_postings": total_postings,
        "avg_postings_per_token": avg_postings,
        "documents_indexed": len(records),
        "total_token_occurrences": total_token_occurrences
    }

    print(f"\n{'='*60}")
    print(f"[InvertedIndex] Build Complete!")
    print(f"  Unique tokens: {len(sorted_postings)}")
    print(f"  Total postings: {total_postings:,}")
    print(f"  Average postings per token: {avg_postings:.2f}")
    print(f"  Documents indexed: {len(records)}")
    print(f"  Total token occurrences: {total_token_occurrences:,}")
    print(f"  Inverted index saved to: {inverted_path}")
    print(f"{'='*60}")

    return stats


if __name__ == "__main__":
    """
    Build inverted index from forward_index.bin
    """
    import sys
    from pathlib import Path
    
    # Try to find forward_index.bin in common locations
    possible_paths = [
        Path("storage") / "forward_index.bin",
        Path("forward_index.bin"),
        Path("../storage/forward_index.bin"),
    ]
    
    forward_path = None
    for path in possible_paths:
        if path.exists():
            forward_path = path
            break
    
    if forward_path is None:
        print("Error: Could not find forward_index.bin")
        print("  Searched in:")
        for path in possible_paths:
            print(f"    - {path.absolute()}")
        print("\n  Please specify the path to forward_index.bin as an argument:")
        print("    python inverted_index.py <path_to_forward_index.bin>")
        sys.exit(1)
    
    default_output = forward_path.parent
    
    try:
        stats = build_inverted_index(forward_path, default_output)
        print(f"\n Inverted index build successful!")
    except FileNotFoundError as e:
        print(f"\n Error: {e}")
    except RuntimeError as e:
        print(f"\n Error: {e}")
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        raise