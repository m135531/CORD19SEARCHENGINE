#!/usr/bin/env python3
"""Validate that postings_index.bin + postings_offsets.bin match aggregated barrel postings.

By default validates a random sample of tokens (safe for large datasets).

Usage:
  python scripts/validate_postings.py --output-dir m:\CORD19DATASET\storage --sample 20
  python scripts/validate_postings.py --output-dir ... --token 17
"""
import argparse
import os
import struct
import random
from collections import defaultdict
from typing import List, Tuple

OFFSETS_NAME = "postings_offsets.bin"
INDEX_NAME = "postings_index.bin"
BARRELS_SUBDIR = "barrels"


def read_offsets(path: str):
    with open(path, "rb") as f:
        data = f.read()
    off = 0
    if len(data) < 4:
        return {}
    count = struct.unpack_from("<I", data, off)[0]
    off += 4
    mapping = {}
    for _ in range(count):
        token_id, offset, length = struct.unpack_from("<IQQ", data, off)
        off += struct.calcsize("<IQQ")
        mapping[token_id] = (offset, length)
    return mapping


def parse_index_block(block: bytes) -> List[Tuple[int, int, List[int]]]:
    # returns list of (doc_id, freq, positions)
    res = []
    off = 0
    if len(block) < 4:
        return res
    doc_count = struct.unpack_from("<I", block, off)[0]
    off += 4
    for _ in range(doc_count):
        if off + 12 > len(block):
            raise ValueError("truncated postings block")
        doc_id, freq, pos_count = struct.unpack_from("<III", block, off)
        off += 12
        positions = []
        if pos_count:
            positions = list(struct.unpack_from(f"<{pos_count}I", block, off))
            off += 4 * pos_count
        res.append((doc_id, freq, positions))
    return res


def aggregate_from_barrels(barrels_dir: str, token_id: int) -> List[Tuple[int, int, List[int]]]:
    results = []
    for fname in sorted(os.listdir(barrels_dir)):
        if not fname.startswith("barrel") or not fname.endswith(".bin"):
            continue
        path = os.path.join(barrels_dir, fname)
        with open(path, "rb") as f:
            while True:
                hdr = f.read(12)
                if len(hdr) < 12:
                    break
                t_id, doc_id, freq = struct.unpack("<III", hdr)
                pos_raw = f.read(4)
                if len(pos_raw) < 4:
                    break
                pos_count = struct.unpack("<I", pos_raw)[0]
                positions = []
                if pos_count:
                    blob = f.read(pos_count * 4)
                    if len(blob) < pos_count * 4:
                        raise ValueError("truncated positions in barrel file")
                    positions = list(struct.unpack(f"<{pos_count}I", blob))
                if t_id == token_id:
                    results.append((doc_id, freq, positions))
    # sort by doc_id for deterministic compare
    results.sort(key=lambda x: x[0])
    return results


def validate_token(output_dir: str, token_id: int) -> bool:
    offsets_path = os.path.join(output_dir, OFFSETS_NAME)
    index_path = os.path.join(output_dir, INDEX_NAME)
    barrels_dir = os.path.join(output_dir, BARRELS_SUBDIR)
    mapping = read_offsets(offsets_path)
    if token_id not in mapping:
        print(f"Token {token_id} not present in offsets file")
        return False
    off, length = mapping[token_id]
    with open(index_path, "rb") as idx:
        idx.seek(off)
        block = idx.read(length)
    postings_block = parse_index_block(block)
    barrel_postings = aggregate_from_barrels(barrels_dir, token_id)

    # normalize shapes and compare
    if len(postings_block) != len(barrel_postings):
        print(f"Mismatch token {token_id}: index has {len(postings_block)} postings, barrels have {len(barrel_postings)}")
        return False
    for a, b in zip(postings_block, barrel_postings):
        if a[0] != b[0] or a[1] != b[1] or a[2] != b[2]:
            print(f"Posting mismatch for token {token_id}: index={a} vs barrels={b}")
            return False
    print(f"Token {token_id} validated: {len(postings_block)} postings match")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--sample", type=int, default=10)
    ap.add_argument("--token", type=int, default=None)
    args = ap.parse_args()

    output_dir = args.output_dir
    offsets_path = os.path.join(output_dir, OFFSETS_NAME)
    if not os.path.exists(offsets_path):
        raise SystemExit("postings_offsets.bin missing; run build_postings_index first")
    mapping = read_offsets(offsets_path)
    token_ids = list(mapping.keys())
    if not token_ids:
        raise SystemExit("no tokens in offsets file")

    to_check = []
    if args.token is not None:
        to_check = [args.token]
    else:
        to_check = random.sample(token_ids, min(args.sample, len(token_ids)))

    all_ok = True
    for t in to_check:
        ok = validate_token(output_dir, t)
        all_ok = all_ok and ok

    if not all_ok:
        raise SystemExit(2)
    print("Validation successful for all sampled tokens")


if __name__ == "__main__":
    main()
