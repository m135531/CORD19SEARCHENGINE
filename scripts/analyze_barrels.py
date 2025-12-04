#!/usr/bin/env python3
"""Analyze barrel mapping and file sizes for the index output.

Usage:
  python scripts/analyze_barrels.py --output-dir m:\CORD19DATASET\storage
"""
import argparse
import os
import struct
import json
from collections import defaultdict


def read_barrel_mapping(path):
    with open(path, "rb") as f:
        data = f.read()
    off = 0
    if len(data) < 12:
        raise ValueError("barrel_mapping.bin too small or missing header")
    num_barrels, special_id, mapping_count = struct.unpack_from("<III", data, off)
    off += 12
    mappings = []
    for _ in range(mapping_count):
        if off + 8 > len(data):
            raise ValueError("unexpected EOF while reading mappings")
        token_id, barrel_id = struct.unpack_from("<II", data, off)
        off += 8
        mappings.append((token_id, barrel_id))
    return num_barrels, special_id, mappings


def analyze(output_dir):
    mapping_file = os.path.join(output_dir, "barrel_mapping.bin")
    barrels_dir = os.path.join(output_dir, "barrels")
    if not os.path.exists(mapping_file):
        print(f"Missing {mapping_file}; run indexing or point to correct output dir")
        return 1
    num_barrels, special_id, mappings = read_barrel_mapping(mapping_file)
    by_barrel = defaultdict(list)
    for token_id, barrel_id in mappings:
        by_barrel[barrel_id].append(token_id)
    results = []
    for barrel_id in sorted(by_barrel.keys()):
        name = f"barrel_{barrel_id:02d}.bin"
        alt = "barrel_freq.bin" if barrel_id == special_id else None
        path1 = os.path.join(barrels_dir, name)
        path2 = os.path.join(barrels_dir, alt) if alt else None
        file_path = path1 if os.path.exists(path1) else (path2 if path2 and os.path.exists(path2) else None)
        size = os.path.getsize(file_path) if file_path and os.path.exists(file_path) else 0
        token_count = len(by_barrel[barrel_id])
        avg_bytes = size / token_count if token_count else 0
        results.append({
            "barrel_id": barrel_id,
            "token_count": token_count,
            "file": os.path.basename(file_path) if file_path else None,
            "size_bytes": size,
            "avg_bytes_per_token": avg_bytes,
        })
    results_sorted = sorted(results, key=lambda r: r["size_bytes"], reverse=True)
    summary = {
        "num_barrels": num_barrels,
        "special_barrel_id": special_id,
        "total_tokens": len(mappings),
        "per_barrel": results_sorted,
    }
    print(json.dumps(summary, indent=2))
    print("\nBarrel summary (largest → smallest):")
    for r in results_sorted:
        print(f" - barrel {r['barrel_id']:02d}: tokens={r['token_count']}, size={r['size_bytes']:,} bytes, avg={r['avg_bytes_per_token']:.1f} bytes/token, file={r['file']}")
    return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default=os.path.join(os.getcwd(), "test_storage"))
    args = p.parse_args()
    return analyze(args.output_dir)


if __name__ == "__main__":
    raise SystemExit(main())
