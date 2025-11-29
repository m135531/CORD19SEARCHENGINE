import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from source.indexer.lexicon import build_lexicon

TEST_DATA_DIR = Path(r"C:\DSAPROJ\CORD19SEARCHENGINE\test_data")

def test_build_lexicon_with_samples(tmp_path):
    sample_dir = tmp_path / "samples"
    shutil.copytree(TEST_DATA_DIR, sample_dir)

    stats = build_lexicon(tmp_path, limit=10)

    assert stats["documents_indexed"] == 10
    assert stats["unique_terms"] > 0
    assert (tmp_path / "lexicon.bin").exists()