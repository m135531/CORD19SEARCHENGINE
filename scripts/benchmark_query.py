#!/usr/bin/env python3
"""Benchmark token lookup using postings_offsets + postings_index.

Measures: time to locate offset (lookup in offsets file) and time to read block from postings_index.bin.
Supports --warm which mmaps files to reduce cold-start variability.

Usage:
  python scripts/benchmark_query.py --output-dir m:\CORD19DATASET\storage --sample 20 [--warm]
"""
import argparse
import os
import struct
import time
import random
import mmap
from typing import Dict, Tuple, List

OFFSETS_NAME = "postings_offsets.bin"
INDEX_NAME = "postings_index.bin"


def read_offsets(path: str) -> Dict[int, Tuple[int, int]]:
    with open(path, "rb") as f:
        buf = f.read()
    off = 0
    if len(buf) < 4:
        return {}
    token_count = struct.unpack_from("<I", buf, off)[0]
    off += 4
    mapping = {}
    for _ in range(token_count):
        token_id, offset, length = struct.unpack_from("<IQQ", buf, off)
        off += struct.calcsize("<IQQ")
        mapping[token_id] = (offset, length)
    return mapping


def read_index_block(path: str, offset: int, length: int, use_mmap: bool = False, mm=None) -> bytes:
    if use_mmap and mm is not None:
        return mm[offset:offset+length]
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read(length)


def parse_block(block: bytes) -> int:
    # returns number of postings (doc_count) parsed
    if len(block) < 4:
        return 0
    doc_count = struct.unpack_from("<I", block, 0)[0]
    return doc_count


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--sample", type=int, default=10)
    ap.add_argument("--warm", action="store_true")
    args = ap.parse_args()

    offsets_path = os.path.join(args.output_dir, OFFSETS_NAME)
    index_path = os.path.join(args.output_dir, INDEX_NAME)
    if not os.path.exists(offsets_path) or not os.path.exists(index_path):
        raise SystemExit("postings_offsets.bin or postings_index.bin missing in output_dir")

    mapping = read_offsets(offsets_path)
    token_ids = list(mapping.keys())
    if not token_ids:
        raise SystemExit("no tokens found in offsets file")

    sample_tokens = random.sample(token_ids, min(args.sample, len(token_ids)))

    use_mmap = args.warm
    mm = None
    if use_mmap:
        f = open(index_path, "rb")
        mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)

    results = []
    for token_id in sample_tokens:
        t0 = time.perf_counter()
        off, length = mapping[token_id]
        t1 = time.perf_counter()
        locate_ms = (t1 - t0) * 1000.0

        t2 = time.perf_counter()
        block = read_index_block(index_path, off, length, use_mmap=use_mmap, mm=mm)
        t3 = time.perf_counter()
        read_ms = (t3 - t2) * 1000.0
        doc_count = parse_block(block)
        results.append((token_id, locate_ms, read_ms, doc_count, length))

    if mm is not None:
        mm.close()

    print("Benchmark (sample tokens):")
    for token_id, locate_ms, read_ms, doc_count, length in results:
        print(f"token={token_id} locate={locate_ms:.3f}ms read={read_ms:.3f}ms docs={doc_count} bytes={length}")


if __name__ == "__main__":
    main()
