"""
Unified CORD-19 Search Engine Indexer
=====================================
STUDY NOTES:
This pipeline converts raw text documents into a searchable data structure.
It follows a standard Information Retrieval (IR) flow:

1.  **Lexicon Building:** Assigning a unique ID to every unique word.
    (e.g., "virus" -> 105, "cell" -> 402)

2.  **Forward Index:** Converting documents into lists of these IDs.
    (Document 1 -> [105, 402, ...])

3.  **Inverted Index:** The reverse map for searching.
    (Who contains 105? -> [Doc 1, Doc 50, Doc 99])
    *Note: The Inverted Index uses 'Bucketing' to avoid running out of RAM.*
"""

import json
import struct  # KEY LIBRARY: Used to pack numbers into C-style binary bytes (saves massive space)
import unicodedata
import heapq   # Used for merging sorted files (External Merge Sort)
import shutil
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional, BinaryIO, Sequence, Set
from collections import defaultdict

# ==========================================
# CONFIGURATION
# ==========================================
# UPDATE THIS LINE IN YOUR CODE:
DATASET_ROOT_DIR = Path(r"C:\Users\Administrator\Downloads\cord-19_2020-06-08(dataset)\2020-06-08\document_parses\document_parses")
OUTPUT_DIR = Path(r"C:\Users\Administrator\Downloads\rdm.git\CORD19SEARCHENGINE\storage")
STOPWORDS_PATH = None

# Performance Tuning
LOG_EVERY = 50
# BUCKET_COUNT is crucial for the Inverted Index.
# We split data into 128 smaller files so we don't crash RAM when sorting.
BUCKET_COUNT = 128

# ==========================================
# PART 1: SHARED TEXT PROCESSING & LEXICON
# ==========================================

def load_stopwords() -> Set[str]:
    """
    Loads common words (stopwords) that we want to ignore (like 'the', 'and').
    Using a SET is important here because checking 'if x in set' is O(1) (instant),
    whereas checking a list is O(n) (slow).
    """
    def _norm(word: str) -> str:
        # NFKC normalization is better than simple .lower().
        # It handles weird unicode characters (e.g., turning a combined 'fi' ligature into 'f' and 'i').
        return unicodedata.normalize("NFKC", word).lower()

    base = {
        "a","an","the","and","or","but","if","while","to","of","in","for",
        "on","with","as","by","is","it","this","that","be","are","from"
    }
    normalized = {_norm(word) for word in base}

    # Optional: Load extra stopwords from a file if provided
    if STOPWORDS_PATH and Path(STOPWORDS_PATH).exists():
        normalized |= {
            _norm(line.strip())
            for line in Path(STOPWORDS_PATH).read_text().splitlines()
            if line.strip()
        }
    return normalized

def normalize_text(sections: List[Dict]) -> str:
    """Helper to smash all text sections (abstract, body, etc.) into one long string."""
    return "\n".join(block.get("text", "") for block in sections if block.get("text"))

def tokenize(text: str, stopwords: Set[str]) -> List[str]:
    """
    Splits text into words.
    Instead of using regex (which can be slow), this iterates character by character.
    It accumulates chars into 'current' and flushes them when a non-alphanumeric char is hit.
    """
    normalized = unicodedata.normalize("NFKC", text).lower()
    tokens: List[str] = []
    current: List[str] = []

    def _flush_current() -> None:
        if current:
            token = "".join(current)
            if token and token not in stopwords:
                tokens.append(token)
            current.clear()

    for char in normalized:
        if char.isalnum():
            current.append(char)
        else:
            _flush_current()

    _flush_current() # Catch the last word if the text doesn't end with a space
    return tokens

class Lexicon:
    """
    The Dictionary of the search engine.
    It maps Strings <-> Integers.
    We do this because processing integers (4 bytes) is way faster than processing strings.
    """
    def __init__(self):
        self.word2id: Dict[str, int] = {}  # Fast lookup: "virus" -> 5
        self.id2word: List[str] = []       # Fast lookup: 5 -> "virus"

    def get_id(self, token: str, create_if_missing: bool = True) -> int:
        """Returns the ID for a word. If we haven't seen it before, assign a new ID."""
        if token not in self.word2id:
            if not create_if_missing:
                return -1
            token_id = len(self.id2word)
            self.word2id[token] = token_id
            self.id2word.append(token)
        return self.word2id[token]

    def write_binary(self, path: Path) -> None:
        """
        Saves the lexicon to disk.
        Uses 'struct.pack' to write binary data.
        '<I' means: Little-Endian byte order, Unsigned Integer (4 bytes).
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            # First, tell the file how many words total are in the lexicon
            f.write(struct.pack("<I", len(self.id2word)))
            for token_id, token in enumerate(self.id2word):
                encoded = token.encode("utf-8")
                # Write length of string, the string bytes, then the ID
                f.write(struct.pack("<I", len(encoded)))
                f.write(encoded)
                f.write(struct.pack("<I", token_id))

def iter_source_files(root_dir: Path) -> Iterable[Tuple[str, Path]]:
    """
    Generator function.
    NOTE: This uses 'yield'. This is critical.
    If we returned a list of 150k files, we might choke memory.
    Yielding handles one file at a time.
    """
    pmc_dir = root_dir / "pmc_json"
    pdf_dir = root_dir / "pdf_json"

    seen = set()
    # Prioritize PMC files (usually cleaner XML parses) over PDF parses
    for tag, folder in (("pmc", pmc_dir), ("pdf", pdf_dir)):
        if not folder.exists():
            continue
        for json_path in sorted(folder.glob("*.json")):
            paper_id = json_path.stem.split(".")[0]
            if paper_id in seen:
                continue
            seen.add(paper_id)
            yield tag, json_path

def process_document(json_path: Path) -> Dict:
    """Parses the JSON structure of the dataset."""
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    metadata = data.get("metadata") or {}
    abstract_text = normalize_text(data.get("abstract") or [])
    body_text = normalize_text(data.get("body_text") or [])

    return {
        "paper_id": data.get("paper_id") or json_path.stem,
        "title": metadata.get("title") or "",
        "text": f"{abstract_text}\n{body_text}".strip()
    }

# ==========================================
# PART 2: FORWARD INDEX BUILDER
# ==========================================

def _serialize_forward_index(records: Sequence[Tuple[int, List[int]]], output_path: Path) -> None:
    """
    Writes the forward index to disk.
    Format: [Total Docs] -> [DocID, NumTokens, TokenID, TokenID...]
    This is extremely compact compared to a database.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as f:
        f.write(struct.pack("<I", len(records)))
        for doc_id, token_ids in records:
            # Write Doc ID and How many tokens it has
            f.write(struct.pack("<II", doc_id, len(token_ids)))
            if token_ids:
                # Write all token IDs at once using * unpacking
                f.write(struct.pack(f"<{len(token_ids)}I", *token_ids))

def _write_doc_metadata(metadata: Sequence[Tuple[int, str]], output_path: Path) -> None:
    """Keeps a simple text file mapping Internal ID (0, 1, 2) to Real Paper ID (SHA hash)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for doc_id, paper_id in metadata:
            handle.write(f"{doc_id}\t{paper_id}\n")

def build_forward_index(input_dir: Path, output_dir: Path, limit: Optional[int] = None) -> Path:
    """
    Step 1 Main Logic:
    1. Reads every file.
    2. Tokenizes text.
    3. Converts words to IDs (updating Lexicon).
    4. Stores the sequence of IDs (Forward Index).
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"Dataset directory not found: {input_dir}")

    # This list will hold the entire structure in memory before writing.
    # Note: For 150k docs, this might get heavy (approx 1-2GB RAM).
    forward_records: List[Tuple[int, List[int]]] = []
    doc_metadata: List[Tuple[int, str]] = []

    stopwords = load_stopwords()
    lex = Lexicon()

    doc_count = 0
    total_tokens = 0
    docs_skipped = 0

    print(f"\n[Step 1] Building Forward Index & Lexicon from {input_dir}")

    for _source_tag, json_path in iter_source_files(input_dir):
        try:
            doc = process_document(json_path)
            tokens = tokenize(doc["text"], stopwords)

            if not tokens:
                docs_skipped += 1
                continue

            doc_id = doc_count
            # CRITICAL: This is where we swap Strings for Integers
            token_ids = [lex.get_id(token) for token in tokens]

            forward_records.append((doc_id, token_ids))
            doc_metadata.append((doc_id, doc["paper_id"]))
            total_tokens += len(token_ids)
            doc_count += 1

            if doc_count % LOG_EVERY == 0:
                print(f"  Processed {doc_count} docs, vocab={len(lex.id2word)}")

            if limit and doc_count >= limit:
                break
        except Exception as e:
            print(f"  Warning: Error processing {json_path.name}: {e}")
            docs_skipped += 1
            continue

    if not forward_records:
        raise RuntimeError("No documents were indexed.")

    # Save everything to disk so Step 2 can read it
    output_dir.mkdir(parents=True, exist_ok=True)
    forward_path = output_dir / "forward_index.bin"
    lexicon_path = output_dir / "lexicon.bin"

    _serialize_forward_index(forward_records, forward_path)
    lex.write_binary(lexicon_path)
    _write_doc_metadata(doc_metadata, output_dir / "doc_ids.tsv")

    print(f"[Step 1 Complete] Indexed {doc_count} docs. Lexicon size: {len(lex.id2word)}")
    return forward_path

# ==========================================
# PART 3: INVERTED INDEX BUILDER
# ==========================================

def _compact_bucket(bucket_path: Path, output_path: Path) -> Tuple[int, int]:
    """
    Reads a raw bucket file (TokenID -> DocID pairs),
    Sorts them in memory,
    And writes them out as: TokenID -> [List of DocIDs] (The posting list).
    """
    postings: Dict[int, set] = defaultdict(set)
    with bucket_path.open("rb") as handle:
        chunk = handle.read(8)
        while chunk:
            if len(chunk) < 8:
                break
            # Unpack exactly 2 integers
            token_id, doc_id = struct.unpack("<II", chunk)
            postings[token_id].add(doc_id)
            chunk = handle.read(8)

    if not postings:
        output_path.touch()
        return 0, 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as handle:
        for token_id in sorted(postings):
            docs = sorted(postings[token_id]) # Ensure doc IDs are sorted for faster intersection later
            handle.write(struct.pack("<II", token_id, len(docs)))
            if docs:
                handle.write(struct.pack(f"<{len(docs)}I", *docs))

    postings_count = sum(len(doc_ids) for doc_ids in postings.values())
    return len(postings), postings_count

def _read_bucket_record(handle: BinaryIO) -> Optional[Tuple[int, List[int]]]:
    """Helper to read one record from a sorted bucket file."""
    header = handle.read(8)
    if not header:
        return None
    token_id, doc_freq = struct.unpack("<II", header)
    doc_ids: List[int] = []
    if doc_freq:
        payload = handle.read(4 * doc_freq)
        doc_ids = list(struct.unpack(f"<{doc_freq}I", payload))
    return token_id, doc_ids

def _merge_bucket_streams(bucket_files: List[Optional[Path]], inverted_path: Path) -> Tuple[int, int]:
    """
    THE HARDEST PART OF THE CODE: External Merge Sort.
    We have 128 sorted bucket files. We need to merge them into one final file.
    We open all 128 files at once and use a Priority Queue (heap) to always
    pick the smallest TokenID from the available open files.
    """
    handles: List[Optional[BinaryIO]] = []
    heap: List[Tuple[int, int, List[int]]] = [] # (token_id, bucket_index, doc_ids)

    # Open all buckets
    for idx, path in enumerate(bucket_files):
        if path is None or not path.exists() or path.stat().st_size == 0:
            handles.append(None)
            continue
        handle = path.open("rb")
        record = _read_bucket_record(handle)
        if record:
            # Push first item of each bucket to heap
            heapq.heappush(heap, (record[0], idx, record[1]))
            handles.append(handle)
        else:
            handle.close()
            handles.append(None)

    inverted_path.parent.mkdir(parents=True, exist_ok=True)
    tokens_written = 0
    total_postings = 0

    with inverted_path.open("wb") as final_handle:
        final_handle.write(struct.pack("<I", 0)) # Placeholder for total count

        # While we still have data in the heap...
        while heap:
            # Pop the smallest token_id available across all buckets
            token_id, bucket_idx, doc_ids = heapq.heappop(heap)
            doc_freq = len(doc_ids)

            # Write to final index
            final_handle.write(struct.pack("<II", token_id, doc_freq))
            if doc_freq:
                final_handle.write(struct.pack(f"<{doc_freq}I", *doc_ids))

            tokens_written += 1
            total_postings += doc_freq

            # Go back to the bucket we just popped from and get its next item
            bucket_handle = handles[bucket_idx]
            if bucket_handle is None:
                continue

            record = _read_bucket_record(bucket_handle)
            if record:
                heapq.heappush(heap, (record[0], bucket_idx, record[1]))
            else:
                # Bucket is empty, close it
                bucket_handle.close()
                handles[bucket_idx] = None

        # Go back to start of file and write the true count of tokens
        final_handle.seek(0)
        final_handle.write(struct.pack("<I", tokens_written))

    # Cleanup any remaining open files
    for handle in handles:
        if handle is not None:
            handle.close()

    return tokens_written, total_postings

def build_inverted_index(forward_index_path: Path, output_dir: Path, num_buckets: int = BUCKET_COUNT) -> None:
    """
    Step 2 Main Logic:
    Converting Forward Index (Doc -> Words) into Inverted Index (Word -> Docs).

    PROBLEM: We can't load the whole thing into RAM to invert it.
    SOLUTION: 'Sharding' or 'Bucketing'.
    1. Stream Forward Index.
    2. Send each (Token, Doc) pair to a specific bucket file based on TokenID % 128.
    3. Sort each small bucket in RAM.
    4. Merge sorted buckets into final file.
    """
    if not forward_index_path.exists():
        raise FileNotFoundError(f"Forward index not found: {forward_index_path}")

    print(f"\n[Step 2] Building Inverted Index from {forward_index_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = output_dir / "tmp_inverted"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Create handles for 128 temporary bucket files
    bucket_paths = [tmp_dir / f"bucket_{idx:03}.bin" for idx in range(num_buckets)]
    bucket_handles = [path.open("wb") for path in bucket_paths]

    try:
        # Pass 1: Read Forward Index and distribute to buckets
        with forward_index_path.open("rb") as forward_handle:
            header = forward_handle.read(4)
            expected_docs = struct.unpack("<I", header)[0]
            docs_seen = 0

            while docs_seen < expected_docs:
                doc_header = forward_handle.read(8)
                if not doc_header: break
                doc_id, token_count = struct.unpack("<II", doc_header)
                token_bytes = forward_handle.read(4 * token_count)

                if token_count:
                    token_ids = struct.unpack(f"<{token_count}I", token_bytes)
                    for token_id in token_ids:
                        # Hashing logic: Distribute tokens evenly across buckets
                        bucket_idx = token_id % num_buckets
                        bucket_handles[bucket_idx].write(struct.pack("<II", token_id, doc_id))

                docs_seen += 1
                if docs_seen % LOG_EVERY == 0:
                    print(f"  Bucketed {docs_seen}/{expected_docs} documents")

    finally:
        for handle in bucket_handles:
            handle.close()

    print(f"  Deduplicating {num_buckets} buckets...")
    bucket_posting_files: List[Optional[Path]] = []

    # Pass 2: Sort each bucket individually
    for path in bucket_paths:
        if not path.exists() or path.stat().st_size == 0:
            bucket_posting_files.append(None)
            continue
        compact_path = path.with_suffix(".postings")
        _compact_bucket(path, compact_path)
        bucket_posting_files.append(compact_path)

    # Pass 3: Merge all sorted buckets
    inverted_path = output_dir / "inverted_index.bin"
    tokens_written, total_postings = _merge_bucket_streams(bucket_posting_files, inverted_path)

    # Cleanup temp files
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(f"[Step 2 Complete] Final Inverted Index Saved: {inverted_path}")
    print(f"  Total Postings: {total_postings}, Unique Tokens: {tokens_written}")

# ==========================================
# MAIN DRIVER
# ==========================================

def main():
    print("=== UNIFIED SEARCH ENGINE INDEXER ===")
    print(f"Dataset Source: {DATASET_ROOT_DIR}")
    print(f"Output Storage: {OUTPUT_DIR}")

    if not DATASET_ROOT_DIR.exists():
        print(f"ERROR: The dataset path does not exist: {DATASET_ROOT_DIR}")
        print("Please edit the 'DATASET_ROOT_DIR' variable at the top of the script.")
        return

    # Phase 1: Create the intermediate forward index
    try:
        forward_index_path = build_forward_index(DATASET_ROOT_DIR, OUTPUT_DIR)
    except Exception as e:
        print(f"CRITICAL ERROR in Step 1: {e}")
        return

    # Phase 2: Create the final inverted index (what we actually query)
    try:
        build_inverted_index(forward_index_path, OUTPUT_DIR)
    except Exception as e:
        print(f"CRITICAL ERROR in Step 2: {e}")
        return

    print("\n=== ALL OPERATIONS COMPLETED SUCCESSFULLY ===")

if __name__ == "__main__":
    main()