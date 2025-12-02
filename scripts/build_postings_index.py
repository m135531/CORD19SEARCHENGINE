#!/usr/bin/env python3
"""Build a token-centric postings index + offsets from existing barrel files.

This implements a streaming, spill-to-disk approach so memory stays bounded.

Outputs (atomic replace):
- postings_index.bin  (concatenated blocks per token)
- postings_offsets.bin (header + entries (uint32 token_count) then per-token: uint32 token_id, uint64 offset, uint64 length)

Usage:
  python scripts/build_postings_index.py --output-dir m:\CORD19DATASET\storage
"""
from __future__ import annotations
import argparse
import os
import struct
import tempfile
import time
from collections import defaultdict
from typing import Dict, List, Tuple

# Tunables
PER_TOKEN_INMEM_THRESHOLD = 1024  # when a token accumulates this many postings, spill to disk
SCAN_LOG_EVERY = 10000  # log every N records during barrel scanning
WRITE_LOG_EVERY = 1000  # log every N tokens during write phase

BARRELS_SUBDIR = "barrels"
POSTINGS_INDEX = "postings_index.bin"
POSTINGS_OFFSETS = "postings_offsets.bin"


def stream_barrel_records(path: str):
    """Yield postings from a barrel without loading whole file into RAM.

    Yields: (token_id:int, doc_id:int, freq:int, positions:List[int])
    """
    with open(path, "rb") as f:
        while True:
            hdr = f.read(12)
            if len(hdr) < 12:
                break
            token_id, doc_id, freq = struct.unpack("<III", hdr)
            pos_count_raw = f.read(4)
            if len(pos_count_raw) < 4:
                break
            pos_count = struct.unpack("<I", pos_count_raw)[0]
            positions = []
            if pos_count:
                blob = f.read(pos_count * 4)
                if len(blob) < pos_count * 4:
                    break
                positions = list(struct.unpack(f"<{pos_count}I", blob))
            yield token_id, doc_id, freq, positions


def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def scan_barrels(barrels_dir: str, tmp_dir: str):
    """Scan barrels and accumulate postings per token, spilling to per-token temp files as needed.

    Returns:
      token_inmem: Dict[token_id, List[(doc_id,freq,positions)]]
      token_disk_counts: Dict[token_id, int]  # number of postings written to disk
      seen_tokens: set
    """
    token_inmem: Dict[int, List[Tuple[int, int, List[int]]]] = defaultdict(list)
    token_disk_counts: Dict[int, int] = defaultdict(int)
    seen_tokens = set()

    barrel_files = [f for f in sorted(os.listdir(barrels_dir)) if f.startswith("barrel") and f.endswith(".bin")]
    if not barrel_files:
        raise SystemExit("No barrel files found; run indexer first.")

    total_start = time.time()
    total_records = 0
    total_spills = 0

    for idx, fname in enumerate(barrel_files, 1):
        path = os.path.join(barrels_dir, fname)
        file_size_mb = os.path.getsize(path) / (1024 * 1024) if os.path.exists(path) else 0
        print(f"\n[SCAN] {idx}/{len(barrel_files)} Processing {fname} ({file_size_mb:.2f} MB)")
        
        barrel_start = time.time()
        records_in_barrel = 0
        next_log = SCAN_LOG_EVERY
        
        for token_id, doc_id, freq, positions in stream_barrel_records(path):
            seen_tokens.add(token_id)
            token_inmem[token_id].append((doc_id, freq, positions))
            records_in_barrel += 1
            total_records += 1
            
            # Progress logging within barrel
            if records_in_barrel >= next_log:
                elapsed = time.time() - barrel_start
                rate = records_in_barrel / elapsed if elapsed > 0 else 0
                print(f"  [SCAN] {fname}: {records_in_barrel:,} records read ({rate:.0f} rec/s)", end="\r")
                next_log += SCAN_LOG_EVERY
            
            # spill if too many in memory for this token
            if len(token_inmem[token_id]) >= PER_TOKEN_INMEM_THRESHOLD:
                tmp_path = os.path.join(tmp_dir, f"token_{token_id}.bin")
                with open(tmp_path, "ab") as tf:
                    for (d, fr, pos) in token_inmem[token_id]:
                        # write per-record: doc_id, freq, pos_count, positions...
                        tf.write(struct.pack("<III", d, fr, len(pos)))
                        if pos:
                            tf.write(struct.pack(f"<{len(pos)}I", *pos))
                        
                token_disk_counts[token_id] += len(token_inmem[token_id])
                token_inmem[token_id].clear()
                total_spills += 1
        
        barrel_elapsed = time.time() - barrel_start
        rate = records_in_barrel / barrel_elapsed if barrel_elapsed > 0 else 0
        print(f"  [SCAN] {fname}: {records_in_barrel:,} records in {barrel_elapsed:.1f}s ({rate:.0f} rec/s)")

    total_elapsed = time.time() - total_start
    print(f"\n[SCAN] Completed scanning {len(barrel_files)} barrels:")
    print(f"  Total records: {total_records:,}")
    print(f"  Unique tokens: {len(seen_tokens):,}")
    print(f"  Tokens with disk spills: {len(token_disk_counts):,} ({total_spills} spill operations)")
    print(f"  Total time: {total_elapsed:.1f}s ({total_records/total_elapsed:.0f} rec/s overall)")
    
    return token_inmem, token_disk_counts, seen_tokens


def write_postings_index(output_dir: str, token_inmem: Dict[int, List[Tuple[int, int, List[int]]]], token_disk_counts: Dict[int, int], seen_tokens: set, tmp_dir: str):
    index_tmp = os.path.join(output_dir, POSTINGS_INDEX + ".tmp")
    offsets_tmp = os.path.join(output_dir, POSTINGS_OFFSETS + ".tmp")
    ensure_dir(output_dir)

    offsets_records: List[Tuple[int, int, int]] = []  # (token_id, offset, length)
    total_tokens = len(seen_tokens)
    write_start = time.time()

    print(f"\n[WRITE] Writing postings_index.bin ({total_tokens:,} tokens)...")
    
    with open(index_tmp, "wb") as idx_f:
        # iterate tokens in sorted order
        for n, token_id in enumerate(sorted(seen_tokens), 1):
            # determine total count: disk_count + inmem
            disk_count = token_disk_counts.get(token_id, 0)
            inmem_list = token_inmem.get(token_id, [])
            total_count = disk_count + len(inmem_list)
            if total_count == 0:
                continue

            start_off = idx_f.tell()
            # write doc_count header
            idx_f.write(struct.pack("<I", total_count))

            # first, stream disk file if present
            tmp_path = os.path.join(tmp_dir, f"token_{token_id}.bin")
            if disk_count and os.path.exists(tmp_path):
                with open(tmp_path, "rb") as tf:
                    # read sequentially the spilled records and write them into idx
                    while True:
                        hdr = tf.read(12)
                        if len(hdr) < 12:
                            break
                        d, fr, pos_count = struct.unpack("<III", hdr)
                        idx_f.write(hdr)
                        if pos_count:
                            blob = tf.read(pos_count * 4)
                            idx_f.write(blob)

            # then, write remaining in-memory postings
            for (d, fr, pos) in inmem_list:
                idx_f.write(struct.pack("<III", d, fr, len(pos)))
                if pos:
                    idx_f.write(struct.pack(f"<{len(pos)}I", *pos))

            end_off = idx_f.tell()
            offsets_records.append((token_id, start_off, end_off - start_off))

            # Progress logging
            if n % WRITE_LOG_EVERY == 0 or n == total_tokens:
                elapsed = time.time() - write_start
                pct = (n / total_tokens * 100) if total_tokens > 0 else 0
                rate = n / elapsed if elapsed > 0 else 0
                index_size_mb = idx_f.tell() / (1024 * 1024)
                print(f"  [WRITE] {n:,}/{total_tokens:,} tokens ({pct:.1f}%) | "
                      f"{index_size_mb:.2f} MB written | {rate:.0f} tokens/s", end="\r")

    print()  # New line after progress
    write_elapsed = time.time() - write_start
    final_size_mb = os.path.getsize(index_tmp) / (1024 * 1024) if os.path.exists(index_tmp) else 0
    print(f"[WRITE] Index file size: {final_size_mb:.2f} MB | Time: {write_elapsed:.1f}s")

    # write offsets tmp
    print(f"[WRITE] Writing postings_offsets.bin ({len(offsets_records):,} entries)...")
    offsets_start = time.time()
    with open(offsets_tmp, "wb") as off_f:
        off_f.write(struct.pack("<I", len(offsets_records)))
        for token_id, off_val, length in offsets_records:
            off_f.write(struct.pack("<IQQ", token_id, off_val, length))
    offsets_elapsed = time.time() - offsets_start
    offsets_size_mb = os.path.getsize(offsets_tmp) / (1024 * 1024) if os.path.exists(offsets_tmp) else 0
    print(f"[WRITE] Offsets file size: {offsets_size_mb:.2f} MB | Time: {offsets_elapsed:.1f}s")

    # atomic replace
    print("[WRITE] Atomic replace of temporary files...")
    os.replace(index_tmp, os.path.join(output_dir, POSTINGS_INDEX))
    os.replace(offsets_tmp, os.path.join(output_dir, POSTINGS_OFFSETS))

    # cleanup tmp token files
    print("[WRITE] Cleaning up temporary token files...")
    cleanup_start = time.time()
    cleaned = 0
    for token_id in sorted(seen_tokens):
        tmp_path = os.path.join(tmp_dir, f"token_{token_id}.bin")
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
                cleaned += 1
        except Exception:
            pass
    cleanup_elapsed = time.time() - cleanup_start
    print(f"[WRITE] Cleaned {cleaned:,} temp files in {cleanup_elapsed:.1f}s")

    total_elapsed = time.time() - write_start
    print(f"\n[DONE] postings_index + postings_offsets written to {output_dir}")
    print(f"  Total write time: {total_elapsed:.1f}s")
    print(f"  Final index size: {final_size_mb:.2f} MB")
    print(f"  Final offsets size: {offsets_size_mb:.2f} MB")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--tmp-dir", default=None)
    args = ap.parse_args()

    output_dir = args.output_dir
    barrels_dir = os.path.join(output_dir, BARRELS_SUBDIR)
    if not os.path.isdir(barrels_dir):
        raise SystemExit("Barrels directory not found; run indexer first.")

    tmp_dir = args.tmp_dir or os.path.join(output_dir, ".postings_tmp")
    ensure_dir(tmp_dir)

    print("=" * 70)
    print("[BUILD] Starting postings index construction")
    print("=" * 70)
    print(f"[BUILD] Barrels directory: {barrels_dir}")
    print(f"[BUILD] Temp directory: {tmp_dir}")
    print(f"[BUILD] Memory threshold per token: {PER_TOKEN_INMEM_THRESHOLD} postings")
    print()
    
    token_inmem, token_disk_counts, seen_tokens = scan_barrels(barrels_dir, tmp_dir)

    print()
    print("=" * 70)
    print(f"[BUILD] Scan complete: {len(seen_tokens):,} tokens collected")
    print(f"[BUILD] Tokens with disk spills: {len(token_disk_counts):,}")
    print("=" * 70)
    print()
    
    write_postings_index(output_dir, token_inmem, token_disk_counts, seen_tokens, tmp_dir)
    
    print()
    print("=" * 70)
    print("[BUILD] Postings index construction complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
