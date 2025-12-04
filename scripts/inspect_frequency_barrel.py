#!/usr/bin/env python3
"""Inspect tokens in the special frequency barrel."""

import struct
from pathlib import Path

def main():
    # read lexicon.bin to build id2token map
    id2token = {}
    lex_path = Path('tmp_output/lexicon.bin')
    with lex_path.open('rb') as f:
        vocab_size = struct.unpack('<I', f.read(4))[0]
        for _ in range(vocab_size):
            token_len = struct.unpack('<I', f.read(4))[0]
            token = f.read(token_len).decode('utf-8')
            token_id = struct.unpack('<I', f.read(4))[0]
            id2token[token_id] = token

    # read barrel mapping
    mapping_path = Path('tmp_barrels/barrel_mapping.bin')
    with mapping_path.open('rb') as f:
        num_barrels, special_freq_barrel_id = struct.unpack('<II', f.read(8))
        mapping_count = struct.unpack('<I', f.read(4))[0]
        
        special_barrel_tokens = []
        for _ in range(mapping_count):
            token_id, barrel_id = struct.unpack('<II', f.read(8))
            if barrel_id == special_freq_barrel_id:
                word = id2token.get(token_id, f'UNKNOWN_{token_id}')
                special_barrel_tokens.append((token_id, word))
        
        print(f'[Special Frequency Barrel] {len(special_barrel_tokens)} tokens:')
        for token_id, word in sorted(special_barrel_tokens):
            print(f'  {token_id}: {word}')

if __name__ == "__main__":
    main()