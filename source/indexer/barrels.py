"""Barrelized inverted postings builder

Reads the project's `forward_index.bin` (written by `forward_index.py`),
analyzes token document frequencies, assigns tokens to unequal-sized
barrels (rare tokens in early barrels, frequent tokens in later barrels,
with an optional special barrel for extremely frequent tokens), and
writes per-barrel binary files containing postings with frequencies and
positions. Also writes `barrel_mapping.bin` for lookup by token_id.

The forward index format expected is the project's format:
  uint32 doc_count
  repeated per doc:
    uint32 doc_id
    uint32 token_count
    repeated uint32 token_id

Barrel file layout (little-endian, repeated records):
  uint32 token_id
  uint32 doc_id
  uint32 frequency
  uint32 positions_count
  repeated uint32 position

The module exposes a CLI: `python -m source.indexer.barrels --forward <path> --output <outdir>`
"""
from __future__ import annotations

import argparse
import struct
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional

from . import lexicon


LOG_EVERY = 50


def stream_forward_index(path: Path) -> Iterable[Tuple[int, List[int]]]:
    """Stream (doc_id, token_ids) from the forward_index.bin file."""
    with path.open("rb") as f:
        header = f.read(4)
        if len(header) < 4:
            return
        doc_count = struct.unpack_from("<I", header, 0)[0]
        docs_read = 0
        while docs_read < doc_count:
            hdr = f.read(8)
            if len(hdr) < 8:
                break
            doc_id, token_count = struct.unpack("<II", hdr)
            token_blob = f.read(4 * token_count) if token_count else b""
            token_ids = list(struct.unpack(f"<{token_count}I", token_blob)) if token_count else []
            yield doc_id, token_ids
            docs_read += 1


class BarrelAssigner:
    """Assign tokens to barrels based on document-frequency distribution.

    Rare tokens are placed in early barrels; more frequent tokens are placed
    in later barrels. Tokens exceeding `frequent_threshold` fraction of the
    corpus are placed into a special frequency barrel.
    """

    def __init__(self, num_barrels: int, total_docs: int, frequent_threshold: float = 0.05):
        self.num_barrels = num_barrels
        self.total_docs = total_docs
        self.frequent_threshold = frequent_threshold
        self.special_freq_barrel_id = num_barrels  # appended as last file
        self.token_to_barrel: Dict[int, int] = {}
        self.token_doc_counts: Dict[int, int] = {}
        self.most_frequent_tokens: List[int] = []

    def analyze(self, token_doc_counts: Dict[int, int]) -> None:
        self.token_doc_counts = dict(token_doc_counts)
        # sort tokens by increasing doc frequency (rare → frequent)
        sorted_tokens = sorted(self.token_doc_counts.items(), key=lambda kv: kv[1])

        threshold_docs = max(1, int(self.total_docs * self.frequent_threshold))

        # mark extremely frequent tokens
        frequent_ids = {tid for tid, cnt in sorted_tokens if cnt >= threshold_docs}

        # top frequent tokens list for diagnostics
        all_desc = sorted(self.token_doc_counts.items(), key=lambda kv: kv[1], reverse=True)
        self.most_frequent_tokens = [tid for tid, _ in all_desc[:100]]

        # assign frequent tokens to special barrel
        for tid in frequent_ids:
            self.token_to_barrel[tid] = self.special_freq_barrel_id

        # remaining tokens assigned progressively
        remaining = [(tid, cnt) for tid, cnt in sorted_tokens if tid not in frequent_ids]
        total_remaining = len(remaining)
        if total_remaining == 0:
            return

        for idx, (token_id, cnt) in enumerate(remaining):
            percentile = idx / total_remaining
            # progressive mapping: use a concave transform so that rare tokens
            # concentrate in early barrels and frequent ones spread into later barrels
            barrel_idx = int((percentile ** 0.6) * self.num_barrels)
            barrel_idx = min(barrel_idx, self.num_barrels - 1)
            self.token_to_barrel[token_id] = barrel_idx

    def get_barrel(self, token_id: int) -> int:
        return self.token_to_barrel.get(token_id, -1)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            f.write(struct.pack("<II", self.num_barrels, self.special_freq_barrel_id))
            mappings = sorted(self.token_to_barrel.items())
            f.write(struct.pack("<I", len(mappings)))
            for token_id, barrel_id in mappings:
                f.write(struct.pack("<II", token_id, barrel_id))


class BarrelWriter:
    """Write postings into barrel files.

    Creates `num_barrels` files named `barrel_00.bin` .. `barrel_{n}.bin` and
    an extra `barrel_freq.bin` for special high-frequency tokens.
    """

    def __init__(self, output_dir: Path, num_barrels: int):
        self.output_dir = output_dir
        self.barrels_dir = output_dir / "barrels"
        self.barrels_dir.mkdir(parents=True, exist_ok=True)
        self.handles: List[Path] = []
        self.num_barrels = num_barrels
        # create handles
        self._files = []
        for i in range(num_barrels):
            p = self.barrels_dir / f"barrel_{i:02}.bin"
            self._files.append(p.open("wb"))
        # special frequency barrel
        self.freq_path = self.barrels_dir / "barrel_freq.bin"
        self._files.append(self.freq_path.open("wb"))

    def write_posting(self, token_id: int, doc_id: int, freq: int, positions: List[int], barrel_id: Optional[int] = None) -> None:
        num_regular = self.num_barrels
        if barrel_id is None:
            barrel_id = token_id % num_regular if num_regular > 0 else 0
        # map out-of-range to special if present
        if barrel_id < 0 or barrel_id > self.num_barrels:
            barrel_id = self.num_barrels

        fh = self._files[barrel_id]
        # write record: token_id, doc_id, freq, positions_count, positions...
        fh.write(struct.pack("<III", token_id, doc_id, freq))
        fh.write(struct.pack("<I", len(positions)))
        for p in positions:
            fh.write(struct.pack("<I", p))

    def close(self) -> None:
        for fh in self._files:
            try:
                fh.close()
            except Exception:
                pass


def build_barrels(forward_index_path: Path, output_dir: Path, num_barrels: int = 16, frequent_threshold: float = 0.05, log_every: int = LOG_EVERY) -> Dict[str, int]:
    """Main entry: build barrel files from a forward index file.

    Returns basic statistics for logging.
    """
    if not forward_index_path.exists():
        raise FileNotFoundError(f"Forward index not found: {forward_index_path}")

    # PASS 1: collect token -> doc frequency
    token_doc_counts: Dict[int, int] = defaultdict(int)
    total_docs = 0
    print(f"[Barrels] PASS 1: scanning forward index {forward_index_path}")
    for doc_idx, token_ids in stream_forward_index(forward_index_path):
        total_docs += 1
        unique = set(token_ids)
        for tid in unique:
            token_doc_counts[tid] += 1
        if total_docs % log_every == 0:
            print(f"[Barrels] scanned {total_docs} documents, tokens observed={len(token_doc_counts)}")

    if total_docs == 0:
        raise RuntimeError("Forward index empty or unreadable")

    # analyze and assign
    assigner = BarrelAssigner(num_barrels, total_docs, frequent_threshold)
    assigner.analyze(token_doc_counts)
    assigner.save(output_dir / "barrel_mapping.bin")
    mapping_path = output_dir / "barrel_mapping.bin"
    print(f"[Barrels] Barrel mapping saved to {mapping_path}")

    # Diagnostics: tokens per barrel and estimated postings
    barrel_counts: Dict[int, int] = defaultdict(int)
    barrel_postings_est: Dict[int, int] = defaultdict(int)
    for tid, barrel_id in assigner.token_to_barrel.items():
        barrel_counts[barrel_id] += 1
        barrel_postings_est[barrel_id] += token_doc_counts.get(tid, 0)

    print(f"[BARREL ASSIGNMENT] Created {num_barrels + 1} barrels (unequal sizes)")
    for bid in sorted(barrel_counts.keys()):
        name = "special_freq" if bid == assigner.special_freq_barrel_id else f"barrel_{bid:02d}"
        print(f"  {name}: {barrel_counts[bid]} tokens (~{barrel_postings_est[bid]:,} postings estimated)")

    if assigner.most_frequent_tokens:
        print("\n[MOST FREQUENT WORDS] Top 10 for edge-case testing:")
        for rank, tid in enumerate(assigner.most_frequent_tokens[:10], 1):
            # We avoid loading the full lexicon here; show token id and counts
            docc = token_doc_counts.get(tid, 0)
            barrel_id = assigner.get_barrel(tid)
            bname = "special_freq" if barrel_id == assigner.special_freq_barrel_id else f"barrel_{barrel_id:02d}"
            print(f"  {rank}. id={tid} → {docc} docs → {bname}")

    # PASS 2: stream again and write postings with positions
    writer = BarrelWriter(output_dir, num_barrels)
    print("[Barrels] PASS 2: writing barrel postings (includes positions)")
    docs_written = 0
    for doc_idx, token_ids in stream_forward_index(forward_index_path):
        # build positions per token for this doc
        pos_map: Dict[int, List[int]] = defaultdict(list)
        for pos, tid in enumerate(token_ids):
            pos_map[tid].append(pos)

        for tid, positions in pos_map.items():
            freq = len(positions)
            barrel_id = assigner.get_barrel(tid)
            # if barrel_id == -1 fallback to modulo
            if barrel_id == -1:
                barrel_id = None
            writer.write_posting(tid, doc_idx, freq, positions, barrel_id)

        docs_written += 1
        if docs_written % log_every == 0:
            print(f"[Barrels] wrote postings for {docs_written}/{total_docs} documents")

    writer.close()
    stats = {
        "documents_indexed": total_docs,
        "tokens_assigned": len(assigner.token_to_barrel),
        "unique_tokens_seen": len(token_doc_counts),
    }
    print(f"[Barrels] Complete: wrote barrels to {output_dir / 'barrels'}")
    return stats


def main(argv=None):
    p = argparse.ArgumentParser(prog="barrels")
    p.add_argument("--forward", required=True, help="Path to forward_index.bin")
    p.add_argument("--output", required=True, help="Output directory for barrels and mapping")
    p.add_argument("--num-barrels", type=int, default=16)
    p.add_argument("--freq-threshold", type=float, default=0.05)
    p.add_argument("--log-every", type=int, default=LOG_EVERY, help="How often (docs) to print progress updates")
    args = p.parse_args(argv)

    fpath = Path(args.forward)
    out = Path(args.output)
    stats = build_barrels(fpath, out, args.num_barrels, args.freq_threshold, log_every=args.log_every)
    print(stats)


if __name__ == "__main__":
    main()
