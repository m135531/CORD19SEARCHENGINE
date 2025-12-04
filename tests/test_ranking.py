"""Tests for ranking module."""

import pytest
from source.search.ranking import rank_docs, is_relevant, intersection_multiplier, rank_single_query


def test_rank_single_word_text_only():
    """Test ranking with TEXT hits only."""
    docs = [
        (1, [1, 1, 1]),  # 3 TEXT hits
        (2, [1, 1]),     # 2 TEXT hits
        (3, [1]),        # 1 TEXT hit
    ]
    
    ranked = rank_single_query(docs)
    
    # Should rank by hit count (capped at 20)
    assert ranked[0][1] == 1  # doc 1 has most hits
    assert ranked[1][1] == 2  # doc 2 has second most
    assert ranked[2][1] == 3  # doc 3 has least


def test_rank_with_title_bonus():
    """Test TITLE hit gets highest bonus (+50)."""
    docs = [
        (1, [0, 1, 1]),      # TITLE + 2 TEXT = 2 + 50 = 52
        (2, [1, 1, 1, 1]),   # 4 TEXT = 4 (no title bonus)
    ]
    
    ranked = rank_single_query(docs)
    
    # Doc with TITLE should rank higher despite fewer hits
    assert ranked[0][1] == 1
    assert ranked[0][0] > ranked[1][0]


def test_rank_with_url_bonus():
    """Test URL hit gets +30 bonus."""
    docs = [
        (1, [1, 1, 2]),      # 2 TEXT + URL = 2 + 30 = 32
        (2, [1, 1, 1, 1]),   # 4 TEXT = 4
    ]
    
    ranked = rank_single_query(docs)
    
    assert ranked[0][1] == 1
    assert ranked[0][0] > ranked[1][0]


def test_rank_with_authors_bonus():
    """Test AUTHORS hit gets +20 bonus."""
    docs = [
        (1, [1, 1, 3]),      # 2 TEXT + AUTHORS = 2 + 20 = 22
        (2, [1, 1, 1]),      # 3 TEXT = 3
    ]
    
    ranked = rank_single_query(docs)
    
    assert ranked[0][1] == 1


def test_rank_with_tags_bonus():
    """Test TAGS hit gets +30 bonus."""
    docs = [
        (1, [1, 1, 5]),      # 2 TEXT + TAGS = 2 + 30 = 32
        (2, [1, 1, 1, 1]),   # 4 TEXT = 4
    ]
    
    ranked = rank_single_query(docs)
    
    assert ranked[0][1] == 1


def test_rank_text_hit_cap():
    """Test TEXT hits are capped at 20."""
    docs = [
        (1, [1] * 50),   # 50 TEXT hits, capped at 20
        (2, [1] * 20),   # exactly 20 TEXT hits
    ]
    
    ranked = rank_single_query(docs)
    
    # Both should have same score (capped at 20)
    assert ranked[0][0] == ranked[1][0]


def test_rank_no_stacking_bonuses():
    """Test that bonuses don't stack for same field."""
    docs = [
        (1, [0, 0, 0, 1]),   # 4 hits total + 50 (TITLE bonus once)
        (2, [1, 1, 1, 1]),   # 4 TEXT = 4
    ]
    ranked = rank_single_query(docs)
    assert ranked[0][1] == 1
    doc1_score = ranked[0][0]
    assert doc1_score == 4 + 50  # base 4 hits + 50 TITLE bonus


def test_is_relevant_with_title():
    """Test is_relevant detects TITLE hits."""
    hit_list = [0, 1, 1]  # TITLE + TEXT
    assert is_relevant(hit_list) == True


def test_is_relevant_with_url():
    """Test is_relevant detects URL hits."""
    hit_list = [1, 1, 2]  # TEXT + URL
    assert is_relevant(hit_list) == True


def test_is_relevant_text_only():
    """Test is_relevant returns False for TEXT-only."""
    hit_list = [1, 1, 1]  # TEXT only
    assert is_relevant(hit_list) == False


def test_is_relevant_empty():
    """Test is_relevant with empty hit list."""
    assert is_relevant([]) == False


def test_intersection_multiplier_no_intersections():
    """Test multiplier with no intersections (single-word query)."""
    doc = (1, [1, 1])
    intersections = []
    
    multiplier = intersection_multiplier(doc, intersections)
    assert multiplier == 1  # No boost


def test_rank_field_order_processing():
    """Test that hits are processed in correct order: TITLE, TEXT, URL/AUTHORS/TAGS."""
    docs = [
        (1, [0, 1, 1, 2, 3, 5]),  # 6 hits: TITLE, 2xTEXT, URL, AUTHORS, TAGS
    ]
    ranked = rank_single_query(docs)
    expected_score = 6 + 50 + 30 + 20 + 30  # 6 base hits + field bonuses
    assert ranked[0][0] == expected_score


if __name__ == "__main__":
    pytest.main([__file__, "-v"])