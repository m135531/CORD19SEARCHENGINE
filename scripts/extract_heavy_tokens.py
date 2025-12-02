#!/usr/bin/env python3
"""Identify heavy tokens (large postings blocks) and extract doc-only lists.

Usage:
  python scripts/extract_heavy_tokens.py --output-dir C:\...\storage --threshold-kb 128 --top-n 50

For each selected token it writes: storage/heavy/token_<id>.bin containing:
  uint32 doc_count
  repeated: uint32 doc_id, uint32 freq

And writes storage/heavy/manifest.json with metadata.
"""
from __future__ import annotations
import argparse
import os
import struct
import json
from pathlib import Path
from typing import Dict, List, Tuple

OFF = "postings_offsets.bin"
IDX = "postings_index.bin"


def read_offsets(path: Path) -> Dict[int, Tuple[int, int]]:
    with path.open('rb') as f:
        data = f.read()
    off = 0
    if len(data) < 4:
        return {}
    token_count = struct.unpack_from('<I', data, off)[0]
    off += 4
    mapping = {}
    entry_sz = struct.calcsize('<IQQ')
    for i in range(token_count):
        if off + entry_sz > len(data):
            break
        token_id, offset, length = struct.unpack_from('<IQQ', data, off)
        off += entry_sz
        mapping[token_id] = (offset, length)
    return mapping


def parse_postings_block(block: bytes) -> List[Tuple[int,int]]:
    # returns list of (doc_id, freq) ignoring positions
    res = []
    off = 0
    if len(block) < 4:
        return res
    doc_count = struct.unpack_from('<I', block, off)[0]
    off += 4
    for _ in range(doc_count):
        if off + 12 > len(block):
            break
        doc_id, freq, pos_count = struct.unpack_from('<III', block, off)
        off += 12
        # skip positions
        off += pos_count * 4
        res.append((doc_id, freq))
    return res


def write_doc_only(path: Path, postings: List[Tuple[int,int]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as f:
        f.write(struct.pack('<I', len(postings)))
        for doc_id, freq in postings:
            f.write(struct.pack('<II', doc_id, freq))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--output-dir', required=True)
    p.add_argument('--threshold-kb', type=int, default=128)
    p.add_argument('--top-n', type=int, default=50, help='Process top-N heaviest tokens (0 = all)')
    args = p.parse_args()

    out = Path(args.output_dir)
    off_path = out / OFF
    idx_path = out / IDX
    if not off_path.exists() or not idx_path.exists():
        raise SystemExit('postings_offsets.bin or postings_index.bin missing in output-dir')

    mapping = read_offsets(off_path)
    heavy = []
    threshold = args.threshold_kb * 1024
    for tid, (_off, length) in mapping.items():
        if length >= threshold:
            heavy.append((tid, length))
    heavy.sort(key=lambda x: x[1], reverse=True)

    if not heavy:
        print(f'No tokens with block size >= {args.threshold_kb} KB')
        return

    proc = heavy if args.top_n == 0 else heavy[:args.top_n]
    print(f'Found {len(heavy)} heavy tokens; processing {len(proc)} (top_n={args.top_n})')

    manifest = {'threshold_kb': args.threshold_kb, 'processed': [], 'total_heavy': len(heavy)}
    with idx_path.open('rb') as idxf:
        for token_id, length in proc:
            offset, length = mapping[token_id]
            idxf.seek(offset)
            blk = idxf.read(length)
            postings = parse_postings_block(blk)
            out_file = out / 'heavy' / f'token_{token_id}.bin'
            write_doc_only(out_file, postings)
            manifest['processed'].append({'token_id': token_id, 'bytes': length, 'doc_count': len(postings), 'path': str(out_file.relative_to(out))})
            print(f'Extracted token {token_id}: {len(postings)} docs -> {out_file}')

    (out / 'heavy' / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    print('Done. Manifest written to', out / 'heavy' / 'manifest.json')


if __name__ == '__main__':
    main()
