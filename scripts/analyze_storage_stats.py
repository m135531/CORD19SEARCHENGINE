import os
import struct
import statistics
from pathlib import Path

STORAGE = Path(r"C:\DSAPROJ\CORD19SEARCHENGINE\storage")
LEX = STORAGE / "lexicon.bin"
OFF = STORAGE / "postings_offsets.bin"
IDX = STORAGE / "postings_index.bin"
FWD = STORAGE / "forward_index.bin"
BARRELS_DIR = STORAGE / "barrels"

def human(n):
    for u in ['B','KB','MB','GB']:
        if n < 1024.0:
            return f"{n:.2f}{u}"
        n /= 1024.0
    return f"{n:.2f}TB"

out = {}
# file sizes
for p in [LEX, OFF, IDX, FWD]:
    out[p.name] = p.stat().st_size if p.exists() else None

# lexicon vocab size
if LEX.exists():
    data = LEX.read_bytes()
    off = 0
    if len(data) >= 4:
        vocab = struct.unpack_from('<I', data, off)[0]
        out['vocab_size'] = vocab
    else:
        out['vocab_size'] = 0
else:
    out['vocab_size'] = None

# forward index doc count
if FWD.exists():
    with FWD.open('rb') as f:
        hdr = f.read(4)
        if len(hdr) >= 4:
            out['forward_doc_count'] = struct.unpack_from('<I', hdr, 0)[0]
        else:
            out['forward_doc_count'] = 0
else:
    out['forward_doc_count'] = None

# postings offsets analysis
lengths = []
entries = 0
largest = []
if OFF.exists():
    data = OFF.read_bytes()
    off = 0
    if len(data) >= 4:
        token_count = struct.unpack_from('<I', data, off)[0]
        off += 4
        out['postings_token_count'] = token_count
        for i in range(token_count):
            if off + struct.calcsize('<IQQ') > len(data):
                break
            tid, offset, length = struct.unpack_from('<IQQ', data, off)
            off += struct.calcsize('<IQQ')
            lengths.append((tid, length))
        entries = len(lengths)
        if entries:
            lens = [l for (_t,l) in lengths]
            out['postings_index_size'] = IDX.stat().st_size if IDX.exists() else None
            out['tokens_in_offsets'] = entries
            out['avg_block_size'] = statistics.mean(lens)
            out['median_block_size'] = statistics.median(lens)
            out['95pct_block_size'] = statistics.quantiles(lens, n=100)[94]
            out['max_block_size'] = max(lens)
            out['total_blocks_bytes'] = sum(lens)
            # top 10 largest
            largest = sorted(lengths, key=lambda x: x[1], reverse=True)[:10]
            out['top_10_largest'] = [{'token_id':t,'bytes':b} for t,b in largest]
else:
    out['postings_token_count'] = None

# barrels sizes summary
barrel_summary = []
if BARRELS_DIR.exists():
    for fn in sorted(os.listdir(BARRELS_DIR)):
        path = BARRELS_DIR / fn
        if path.is_file():
            barrel_summary.append({'file':fn,'size':path.stat().st_size})
    barrel_summary = sorted(barrel_summary, key=lambda x: x['size'], reverse=True)
out['barrels'] = barrel_summary

# print report
print('STORAGE SUMMARY:')
print(f"lexicon.bin: {human(out.get('lexicon.bin',0))}   vocab={out.get('vocab_size')}")
print(f"postings_offsets.bin: {human(out.get('postings_offsets.bin',0))}   tokens_in_offsets={out.get('postings_token_count')}")
print(f"postings_index.bin: {human(out.get('postings_index.bin',0))}")
print(f"forward_index.bin: {human(out.get('forward_index.bin',0))}   docs={out.get('forward_doc_count')}")
print()
if out.get('tokens_in_offsets'):
    print(f"Blocks: count={out['tokens_in_offsets']} total_bytes={human(out['total_blocks_bytes'])} avg={human(out['avg_block_size'])} median={human(out['median_block_size'])} 95pct={human(out['95pct_block_size'])} max={human(out['max_block_size'])}")
    print('Top 10 largest blocks:')
    for item in out['top_10_largest']:
        print(f"  token_id={item['token_id']}  bytes={human(item['bytes'])}")
else:
    print('No postings offsets present')

print('\nBarrel files (largest 10):')
for b in out['barrels'][:10]:
    print(f"  {b['file']}: {human(b['size'])}")

# Estimates for memory usage if loading offsets and lexicon into Python dicts
est = {}
if out.get('tokens_in_offsets') is not None:
    token_count = out['tokens_in_offsets']
    # rough per-entry overhead estimates: 120 bytes per offset entry in Python dict
    est['offsets_memory_bytes'] = int(token_count * 120)
    # lexicon strings: average token length estimate
    avg_token_len = 10
    est['lexicon_memory_bytes'] = int(out.get('vocab_size',0) * (avg_token_len + 50))
    total_est = est['offsets_memory_bytes'] + est['lexicon_memory_bytes']
    est['total_est_bytes'] = total_est
    print('\nEstimated memory if loading offsets+lexicon into Python:')
    print(f"  offsets ~ {human(est['offsets_memory_bytes'])}, lexicon ~ {human(est['lexicon_memory_bytes'])}, total ~ {human(est['total_est_bytes'])}")

# save JSON to storage for later review
try:
    import json
    (STORAGE / 'storage_stats.json').write_text(json.dumps(out, indent=2))
except Exception:
    pass

