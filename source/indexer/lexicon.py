"""CORD-19 Lexicon Builder

Scan the corpus, tokenize text, and emit a binary ``lexicon.bin``. The module
remains intentionally minimal so we can validate lexicon creation before
layering on forward indexes or barrels.
"""

import json  # JSON parsing for input documents
import re  # regex for tokenization
import struct  # packing/unpacking binary formats
from pathlib import Path  # filesystem path helpers
from typing import Dict, Iterable, List, Tuple, Optional  # type annotations used in function signatures

# CONFIGURATION
ROOT_DIR = Path(r"M:\CORD19DATASET\document_parses")  # base dataset folder
PDF_DIR = ROOT_DIR / "pdf_json"  # pdf-parsed JSON files
PMC_DIR = ROOT_DIR / "pmc_json"  # pmc-parsed JSON files (prefer these when duplicates exist)
OUTPUT_DIR = Path(r"C:\DSAPROJ\CORD19SEARCHENGINE\storage")  # where index outputs are written

STOPWORDS_PATH = None  # path to optional custom stopword list
LOG_EVERY = 50  # log progress every N documents


TOKEN_RE = re.compile(r"[a-z0-9]+")  # pattern used to extract tokens

def load_stopwords() -> set:
    """Return a set of stopwords combining a built-in list and an optional file."""
    base = {
        "a","an","the","and","or","but","if","while","to","of","in","for",
        "on","with","as","by","is","it","this","that","be","are","from"
    }  # minimal default stopword set
    if STOPWORDS_PATH and Path(STOPWORDS_PATH).exists():
        base |= {line.strip().lower() for line in Path(STOPWORDS_PATH).read_text().splitlines()}  # extend from file
    return base

def iter_source_files() -> Iterable[Tuple[str, Path]]:
    """Yield (source_tag, path). Prefers PMC version when duplicate paper_id occurs."""
    seen = set()  # keep seen paper_ids to avoid duplicates
    # Process PMC first (cleaner), then PDF if unseen
    for tag, folder in (("pmc", PMC_DIR), ("pdf", PDF_DIR)):
        for json_path in sorted(folder.glob("*.json")):
            paper_id = json_path.stem.split(".")[0]
            if paper_id in seen:
                continue
            seen.add(paper_id)
            yield tag, json_path

def normalize_text(sections: List[Dict]) -> str:
    """Concatenate a list of section blocks into a single text string."""
    return "\n".join(block.get("text", "") for block in sections if block.get("text"))

def tokenize(text: str, stopwords: set) -> List[str]:
    """Return a list of tokens from text after lowercasing and stopword filtering."""
    return [token for token in TOKEN_RE.findall(text.lower()) if token not in stopwords]

class Lexicon:
    """Simple lexicon mapping tokens to incremental integer ids and back."""

    def __init__(self):
        self.word2id: Dict[str, int] = {}  # token -> id map
        self.id2word: List[str] = []  # id -> token list

    def get_id(self, token: str, create_if_missing: bool = True) -> int:
        """Return the id for token, optionally creating it."""
        if token not in self.word2id:
            if not create_if_missing:
                return -1
            token_id = len(self.id2word)
            self.word2id[token] = token_id
            self.id2word.append(token)
        return self.word2id[token]

    def write_binary(self, path: Path) -> None:
        """Write the lexicon to disk using the binary format described above."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            f.write(struct.pack("<I", len(self.id2word)))
            for token_id, token in enumerate(self.id2word):
                encoded = token.encode("utf-8")
                f.write(struct.pack("<I", len(encoded)))
                f.write(encoded)
                f.write(struct.pack("<I", token_id))



def process_document(json_path: Path) -> Dict:
    """Load a document JSON and return a dict with `paper_id`, `title`, and `text`."""
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
def build_lexicon(output_dir: Path = OUTPUT_DIR, limit: Optional[int] = None) -> Dict[str, int]:
    """Scan the dataset, build the lexicon, and persist the binary file."""
    stopwords = load_stopwords()
    lexicon = Lexicon()
    docs_processed = 0
    total_tokens = 0

    for _source_tag, json_path in iter_source_files():
        doc_raw = process_document(json_path)
        tokens = tokenize(doc_raw["text"], stopwords)
        if not tokens:
            continue

        docs_processed += 1
        total_tokens += len(tokens)
        for token in tokens:
            token_id = lexicon.get_id(token)

        if docs_processed % LOG_EVERY == 0:
            print(f"Indexed {docs_processed} documents…")

        if limit and docs_processed >= limit:
            break

    if not docs_processed:
        raise RuntimeError("No documents were indexed; check data paths.")

    output_dir.mkdir(parents=True, exist_ok=True)
    lexicon.write_binary(output_dir / "lexicon.bin")

    stats = {
        "documents_indexed": docs_processed,
        "unique_terms": len(lexicon.id2word),
        "avg_doc_length": total_tokens / docs_processed,
    }

    return stats


if __name__ == "__main__":
    summary = build_lexicon()
    print(f"Lexicon build complete: {summary['documents_indexed']} docs, {summary['unique_terms']} unique terms.")