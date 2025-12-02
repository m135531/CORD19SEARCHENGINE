#!/usr/bin/env python3
"""Offsets-first single-term retrieval demo.

This script looks up a token in `lexicon.bin`, uses `postings_offsets.bin` to
locate the token's block inside `postings_index.bin`, then parses and prints
the first `--top` doc entries (doc_id and freq) while reporting precise
timings for offset lookup and block parse/read. It uses `mmap` for the index
file when available to avoid unnecessary copies.

Usage:
  python scripts\query_demo.py --output-dir C:\...\storage --term virus --top 10 --mmap

The script deliberately omits any ranking logic — it only retrieves and
measures. It's optimized to avoid unnecessary allocations and to skip
positions (only skips them) unless required for a different path.
"""
from __future__ import annotations

import argparse
import mmap
import os
import struct
import sys
import time
from typing import Dict, Tuple, List

POSTINGS_INDEX = "postings_index.bin"
POSTINGS_OFFSETS = "postings_offsets.bin"
LEXICON = "lexicon.bin"


def find_token_id_in_lexicon(lex_path: str, term: str) -> int | None:
    """Stream `lexicon.bin` and return token_id for `term`, or None.

    The lexicon format in this repo is: uint32 vocab_size then repeated
    [uint32 token_len][bytes token][uint32 token_id]. We scan sequentially
    and stop when we find a match. This avoids building a full in-memory map.
    """
    with open(lex_path, "rb") as fh:
        # read possible header (vocab size) but be tolerant if absent
        hdr = fh.read(4)
        if len(hdr) < 4:
            return None
        try:
            vocab_size = struct.unpack_from("<I", hdr, 0)[0]
        except struct.error:
            return None
        # iterate vocabulary entries; stop early when we match
        for _ in range(vocab_size):
            tl = fh.read(4)
            if len(tl) < 4:
                break
            token_len = struct.unpack_from("<I", tl, 0)[0]
            token_bytes = fh.read(token_len)
            if len(token_bytes) != token_len:
                break
            token = token_bytes.decode("utf-8")
            tidb = fh.read(4)
            if len(tidb) < 4:
                break
            token_id = struct.unpack_from("<I", tidb, 0)[0]
            if token == term:
                return token_id
    return None


def load_postings_offsets(path: str) -> Dict[int, Tuple[int, int]]:
    """Load `postings_offsets.bin` into a dict token_id -> (offset, length).

    The format used here is: uint32 count then repeated [uint32 token_id][uint64 offset][uint64 length]
    (little-endian). The file is modest in size and loading it once is cheap.
    """
    mapping: Dict[int, Tuple[int, int]] = {}
    with open(path, "rb") as fh:
        data = fh.read()
    if len(data) < 4:
        return mapping
    off = 0
    cnt = struct.unpack_from("<I", data, off)[0]
    off += 4
    entry_sz = struct.calcsize("<IQQ")
    for _ in range(cnt):
        if off + entry_sz > len(data):
            break
        token_id, offset_val, length = struct.unpack_from("<IQQ", data, off)
        off += entry_sz
        mapping[token_id] = (int(offset_val), int(length))
    return mapping


def parse_postings_block_from_mv(mv: memoryview, top_n: int = 10) -> Tuple[int, List[Tuple[int, int]]]:
    """Parse a postings block given as a `memoryview` and return (doc_count, first-top_n list).

    We intentionally only collect `(doc_id, freq)` for the first `top_n` entries
    and efficiently skip positions to avoid allocations.
    """
    res: List[Tuple[int, int]] = []
    off = 0
    L = len(mv)
    if L < 4:
        return 0, res
    doc_count = struct.unpack_from("<I", mv, off)[0]
    off += 4
    # iterate once, collect up to top_n entries
    for _ in range(doc_count):
        if off + 12 > L:
            break
        doc_id, freq, pos_count = struct.unpack_from("<III", mv, off)
        off += 12
        # skip positions
        skip = pos_count * 4
        off += skip
        if len(res) < top_n:
            res.append((doc_id, freq))
    return doc_count, res


def read_postings_block_direct(index_path: str, offset: int, length: int, use_mmap: bool, top_n: int):
    """Read postings block either via mmap or direct file read and parse minimal info."""
    if use_mmap:
        with open(index_path, "rb") as fh:
            mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                mv = memoryview(mm)[offset: offset + length]
                result = parse_postings_block_from_mv(mv, top_n)
            finally:
                # release the memoryview before closing mmap to avoid BufferError
                try:
                    del mv
                except Exception:
                    pass
                mm.close()
            return result
    else:
        with open(index_path, "rb") as fh:
            fh.seek(offset)
            blk = fh.read(length)
        mv = memoryview(blk)
        return parse_postings_block_from_mv(mv, top_n)


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default=os.path.join(os.getcwd(), "storage"))
    p.add_argument("--term", required=True)
    p.add_argument("--top", type=int, default=10)
    p.add_argument("--mmap", action="store_true", help="Use mmap for postings_index.bin reads")
    args = p.parse_args(argv)

    out = args.output_dir
    lex_path = os.path.join(out, LEXICON)
    offsets_path = os.path.join(out, POSTINGS_OFFSETS)
    index_path = os.path.join(out, POSTINGS_INDEX)

    if not os.path.exists(lex_path):
        print(f"Missing {LEXICON} in output-dir")
        return 2
    if not os.path.exists(offsets_path) or not os.path.exists(index_path):
        print("Offsets/index files missing in output-dir; cannot do direct lookup")
        return 3

    # 1) token lookup (streaming through lexicon) - time it
    t0 = time.perf_counter()
    token_id = find_token_id_in_lexicon(lex_path, args.term)
    t1 = time.perf_counter()
    if token_id is None:
        print(f"Token '{args.term}' not found in lexicon")
        return 4

    # 2) load offsets (one-time cost) and lookup
    t2 = time.perf_counter()
    mapping = load_postings_offsets(offsets_path)
    off_lookup_start = time.perf_counter()
    item = mapping.get(token_id)
    off_lookup_end = time.perf_counter()
    if item is None:
        print(f"No postings offset for token_id {token_id}")
        return 5
    offset_val, length = item

    # 3) read and parse block
    read_start = time.perf_counter()
    doc_count, first_entries = read_postings_block_direct(index_path, offset_val, length, args.mmap, args.top)
    read_end = time.perf_counter()

    # Print concise results and timings
    print(f"term={args.term!r} token_id={token_id} doc_count={doc_count} bytes={length}")
    print(f"lex_lookup_ms={(t1-t0)*1000:.2f}  offsets_load_ms={(off_lookup_start-t2)*1000:.2f}  offset_lookup_ms={(off_lookup_end-off_lookup_start)*1000:.4f}  read_ms={(read_end-read_start)*1000:.2f}")
    print(f"first {len(first_entries)} postings (doc_id,freq):")
    for doc_id, freq in first_entries:
        print(f"  {doc_id}\t{freq}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
