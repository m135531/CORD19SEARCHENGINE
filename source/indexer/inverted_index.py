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

import heapq
import shutil
import struct
from collections import defaultdict
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Tuple

BUCKET_COUNT = 128  # shard postings on disk to cap memory usage
LOG_EVERY = 50


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
    """Persist postings for test helpers (kept for backwards compatibility)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as handle:
        handle.write(struct.pack("<I", len(postings)))
        for token_id in sorted(postings.keys()):
            docs = postings[token_id]
            handle.write(struct.pack("<II", token_id, len(docs)))
            if docs:
                handle.write(struct.pack(f"<{len(docs)}I", *docs))


def _compact_bucket(bucket_path: Path, output_path: Path) -> Tuple[int, int]:
    """Deduplicate a bucket's (token_id, doc_id) pairs and persist compact postings."""
    postings: Dict[int, set] = defaultdict(set)
    with bucket_path.open("rb") as handle:
        chunk = handle.read(8)
        while chunk:
            if len(chunk) < 8:
                raise RuntimeError(f"Bucket {bucket_path} is truncated")
            token_id, doc_id = struct.unpack("<II", chunk)
            postings[token_id].add(doc_id)
            chunk = handle.read(8)

    if not postings:
        output_path.touch()
        return 0, 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as handle:
        for token_id in sorted(postings):
            docs = sorted(postings[token_id])
            handle.write(struct.pack("<II", token_id, len(docs)))
            if docs:
                handle.write(struct.pack(f"<{len(docs)}I", *docs))

    postings_count = sum(len(doc_ids) for doc_ids in postings.values())
    return len(postings), postings_count


def _read_bucket_record(handle: BinaryIO) -> Optional[Tuple[int, List[int]]]:
    """Read the next (token_id, doc_ids) record from a compact bucket stream."""
    header = handle.read(8)
    if not header:
        return None
    if len(header) < 8:
        raise RuntimeError("Bucket stream ended unexpectedly while reading header")
    token_id, doc_freq = struct.unpack("<II", header)
    doc_ids: List[int] = []
    if doc_freq:
        payload = handle.read(4 * doc_freq)
        if len(payload) < 4 * doc_freq:
            raise RuntimeError("Bucket stream truncated while reading doc IDs")
        doc_ids = list(struct.unpack(f"<{doc_freq}I", payload))
    return token_id, doc_ids


def _merge_bucket_streams(bucket_files: List[Optional[Path]], inverted_path: Path) -> Tuple[int, int]:
    """Merge sorted bucket streams into the final inverted index file."""
    handles: List[Optional[BinaryIO]] = []
    heap: List[Tuple[int, int, List[int]]] = []  # (token_id, bucket_idx, doc_ids)

    for idx, path in enumerate(bucket_files):
        if path is None or not path.exists() or path.stat().st_size == 0:
            handles.append(None)
            continue
        handle = path.open("rb")
        record = _read_bucket_record(handle)
        if record:
            heapq.heappush(heap, (record[0], idx, record[1]))
            handles.append(handle)
        else:
            handle.close()
            handles.append(None)

    inverted_path.parent.mkdir(parents=True, exist_ok=True)
    tokens_written = 0
    total_postings = 0

    with inverted_path.open("wb") as final_handle:
        final_handle.write(struct.pack("<I", 0))  # placeholder vocab size

        while heap:
            token_id, bucket_idx, doc_ids = heapq.heappop(heap)
            doc_freq = len(doc_ids)
            final_handle.write(struct.pack("<II", token_id, doc_freq))
            if doc_freq:
                final_handle.write(struct.pack(f"<{doc_freq}I", *doc_ids))

            tokens_written += 1
            total_postings += doc_freq

            bucket_handle = handles[bucket_idx]
            if bucket_handle is None:
                continue

            record = _read_bucket_record(bucket_handle)
            if record:
                heapq.heappush(heap, (record[0], bucket_idx, record[1]))
            else:
                bucket_handle.close()
                handles[bucket_idx] = None

        final_handle.seek(0)
        final_handle.write(struct.pack("<I", tokens_written))

    for handle in handles:
        if handle is not None:
            handle.close()

    return tokens_written, total_postings


def build_inverted_index(forward_index_path: Path, output_dir: Path, num_buckets: int = BUCKET_COUNT) -> Dict[str, int]:
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

    print(f"[InvertedIndex] Streaming forward index from {forward_index_path}")

    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = output_dir / "tmp_inverted"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    bucket_paths = [tmp_dir / f"bucket_{idx:03}.bin" for idx in range(num_buckets)]
    bucket_handles = [path.open("wb") for path in bucket_paths]

    total_token_occurrences = 0
    docs_seen = 0
    expected_docs = 0

    try:
        with forward_index_path.open("rb") as forward_handle:
            header = forward_handle.read(4)
            if len(header) < 4:
                raise RuntimeError("Forward index is empty or corrupted.")
            expected_docs = struct.unpack("<I", header)[0]
            if expected_docs == 0:
                raise RuntimeError("Forward index is empty; cannot build inverted index.")

            while docs_seen < expected_docs:
                doc_header = forward_handle.read(8)
                if len(doc_header) < 8:
                    raise RuntimeError("Forward index truncated while reading document header.")
                doc_id, token_count = struct.unpack("<II", doc_header)
                token_bytes = forward_handle.read(4 * token_count)
                if len(token_bytes) < 4 * token_count:
                    raise RuntimeError("Forward index truncated while reading token IDs.")

                if token_count:
                    token_ids = struct.unpack(f"<{token_count}I", token_bytes)
                    for token_id in token_ids:
                        bucket_idx = token_id % num_buckets
                        bucket_handles[bucket_idx].write(struct.pack("<II", token_id, doc_id))
                    total_token_occurrences += token_count

                docs_seen += 1
                if docs_seen % LOG_EVERY == 0:
                    print(f"[InvertedIndex] Bucketed {docs_seen}/{expected_docs} documents")

        if docs_seen != expected_docs:
            raise RuntimeError("Forward index document count mismatch; file may be corrupted.")
    finally:
        for handle in bucket_handles:
            handle.close()

    print(f"[InvertedIndex] Deduplicating {num_buckets} bucket(s)...")
    bucket_posting_files: List[Optional[Path]] = []
    unique_tokens = 0

    for path in bucket_paths:
        if not path.exists() or path.stat().st_size == 0:
            bucket_posting_files.append(None)
            continue
        compact_path = path.with_suffix(".postings")
        token_count, _ = _compact_bucket(path, compact_path)
        unique_tokens += token_count
        bucket_posting_files.append(compact_path)

    print(f"[InvertedIndex] Buckets compacted into {unique_tokens} token segments")

    inverted_path = output_dir / "inverted_index.bin"
    tokens_written, total_postings = _merge_bucket_streams(bucket_posting_files, inverted_path)

    shutil.rmtree(tmp_dir, ignore_errors=True)

    avg_postings = total_postings / tokens_written if tokens_written else 0.0
    stats = {
        "unique_tokens": tokens_written,
        "total_postings": total_postings,
        "avg_postings_per_token": avg_postings,
        "documents_indexed": expected_docs,
        "total_token_occurrences": total_token_occurrences,
    }

    print(f"\n{'='*60}")
    print(f"[InvertedIndex] Build Complete!")
    print(f"  Documents indexed: {expected_docs}")
    print(f"  Unique tokens: {tokens_written}")
    print(f"  Total postings: {total_postings:,}")
    print(f"  Average postings per token: {avg_postings:.2f}")
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