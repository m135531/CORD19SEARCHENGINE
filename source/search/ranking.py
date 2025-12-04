"""
Ranking module for CORD-19 search engine.

Ranks documents based on field-specific hit locations and query term intersections.
Implements single-word and multi-word ranking with field weighting.
"""

def rank_docs(docs, intersections=None):
    """
    Rank documents based on hit counts and field locations.
    
    Args:
        docs: List of (doc_id, hit_list) tuples where hit_list contains field-encoded hits
        intersections: List of intersection dicts for multi-word queries (optional)
    
    Returns:
        List of (score, doc_id) tuples sorted by score descending
    
    Hit encoding (hit % 10):
        0 = TITLE
        1 = TEXT
        2 = URL
        3 = AUTHORS
        5 = TAGS
    """
    if intersections is None:
        intersections = []
    
    top_docs = []
    
    for doc in docs:
        doc_id = doc[0]
        hit_list = doc[1]
        
        # Base score: cap TEXT hits at 20 to avoid keyword stuffing
        this_score = min(len(hit_list), 20)
        
        # Apply intersection multiplier if available
        multiplier = intersection_multiplier(doc, intersections) if intersections else 1
        
        # Add bonus for multi-word intersection
        if multiplier > 1:
            this_score += 10
        
        # Track which special fields we've already bonused (prevent stacking)
        added = [False] * 6
        
        # Process hit list: TITLE first, then TEXT, then URL/AUTHORS/TAGS
        i = 0
        step = 1  # iterate forward initially
        
        while i >= 0 and i < len(hit_list):
            hit = hit_list[i]
            hit_field = hit % 10
            
            # Field-specific bonuses (only apply once per field)
            if hit_field == 0:  # TITLE
                if not added[0]:
                    this_score += 50
                    added[0] = True
            elif hit_field == 2:  # URL
                if not added[2]:
                    this_score += 30
                    added[2] = True
            elif hit_field == 3:  # AUTHORS
                if not added[3]:
                    this_score += 20
                    added[3] = True
            elif hit_field == 5:  # TAGS
                if not added[5]:
                    this_score += 30
                    added[5] = True
            
            # Directional iteration logic:
            # When we hit first TEXT, jump to end and iterate backwards
            # to capture URL/AUTHORS/TAGS efficiently
            if hit_field == 1:  # TEXT
                if step == 1:  # first time seeing TEXT
                    i = len(hit_list) - 1
                    step = -1  # switch to backward iteration
                else:  # already iterated backwards, done with metadata
                    break
            else:
                i += step
        
        # Apply multiplier (higher for multi-word matches in special fields)
        this_score *= multiplier
        
        top_docs.append((this_score, doc_id))
    
    return sorted(top_docs, key=lambda x: x[0], reverse=True)


def is_relevant(hit_list):
    """
    Check if a hit list contains relevant (non-TEXT) hits.
    
    A document is relevant for intersection if it has hits in:
    TITLE, URL, AUTHORS, or TAGS (not just body TEXT).
    
    Args:
        hit_list: List of field-encoded hits
    
    Returns:
        True if hit_list contains any non-TEXT hits
    """
    if not hit_list:
        return False
    
    # Check first hit (could be TITLE)
    if hit_list[0] % 10 != 1:
        return True
    
    # Check last hit (could be URL, AUTHORS, TAGS)
    if hit_list[-1] % 10 != 1:
        return True
    
    return False


def intersection_multiplier(doc, intersections):
    """
    Compute multiplier for documents matching multiple query terms.
    
    For multi-word queries, boosts documents that appear in deeper intersections
    (i.e., match more query terms together).
    
    Args:
        doc: (doc_id, hit_list) tuple
        intersections: List of cumulative intersection dicts
    
    Returns:
        Multiplier: 1 (no boost), (i+1)*2 (later intersection, relevant hits),
        or (i+1)*100 (deepest intersection, special field match)
    """
    doc_id = doc[0]
    multiplier = 1
    
    # Check from deepest intersection backwards (all terms -> fewer terms)
    for i in range(len(intersections) - 1, 0, -1):
        if doc_id in intersections[i]:
            # Document is in this intersection
            if intersections[i][doc_id]:
                # Intersection contains relevant (non-TEXT) hits
                # Deeper intersection = higher multiplier
                return (i + 1) * 100
            
            # Intersection contains only TEXT hits
            if multiplier == 1:
                multiplier = (i + 1) * 2
    
    return multiplier


# Single-word ranking entry point
def rank_single_query(docs):
    """
    Rank documents for a single-word query (no intersections).
    
    Args:
        docs: List of (doc_id, hit_list) tuples
    
    Returns:
        List of (score, doc_id) tuples sorted by score descending
    """
    return rank_docs(docs, intersections=None)


def intersect(doc_lists):
    """
    Compute cumulative intersections of multiple query terms.
    
    Args:
        doc_lists: List of doc lists, one per query term.
                   Each doc list is [(doc_id, hit_list), ...]
    
    Returns:
        List of intersection dicts, where intersections[i] contains docs
        that match the first i+1 query terms.
        Each value is True if intersection has relevant (non-TEXT) hits.
    """
    if len(doc_lists) <= 1:
        return []
    
    # Convert each doc list to {doc_id: is_relevant(hit_list)}
    doc_dicts = [
        {doc[0]: is_relevant(doc[1]) for doc in doc_list}
        for doc_list in doc_lists
    ]
    
    intersections = [doc_dicts[0]]
    
    # Compute cumulative intersections
    for i in range(1, len(doc_dicts)):
        this_intersection = {}
        for doc_id in doc_dicts[i]:
            if doc_id in intersections[i - 1]:
                # Doc matches current term AND all previous terms
                both_relevant = (
                    doc_dicts[i][doc_id] and 
                    intersections[i - 1][doc_id]
                )
                this_intersection[doc_id] = both_relevant
        intersections.append(this_intersection)
    
    return intersections


def rank_multi_query(doc_lists_per_word, normalize=True):
    """
    Rank documents for a multi-word query.
    
    Args:
        doc_lists_per_word: Dict mapping word -> [(doc_id, hit_list), ...]
        normalize: If True, divide final score by number of words
    
    Returns:
        List of (score, doc_id) tuples sorted by score descending
    """
    if not doc_lists_per_word:
        return []
    
    query_words = list(doc_lists_per_word.keys())
    doc_lists = [doc_lists_per_word[word] for word in query_words]
    
    # Compute intersections for boost detection
    intersections = intersect(doc_lists)
    
    # Score each document per word
    scores = {}
    
    for word, doc_list in zip(query_words, doc_lists):
        ranked = rank_docs(doc_list, intersections)
        for score, doc_id in ranked:
            scores.setdefault(doc_id, 0)
            scores[doc_id] += score
    
    # Apply intersection boost: documents matching all words get multiplier
    if intersections:
        deepest_intersection = intersections[-1]  # all words matched
        for doc_id in scores:
            if doc_id in deepest_intersection:
                # Document contains all query words
                if deepest_intersection[doc_id]:
                    # Intersection has relevant (non-TEXT) hits
                    scores[doc_id] *= 3  # aggressive boost for all-word match in special fields
                else:
                    # Intersection only has TEXT hits
                    scores[doc_id] *= 1.5  # modest boost
    
    # Normalize by number of query words (optional)
    if normalize and len(query_words) > 1:
        scores = {
            doc_id: score / len(query_words)
            for doc_id, score in scores.items()
        }
    
    # Sort and return
    final_ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [(score, doc_id) for doc_id, score in final_ranked]


def rank_query(query_words, doc_lists_per_word):
    """
    Unified entry point for single or multi-word ranking.
    
    Args:
        query_words: List of query terms (can be length 1 or more)
        doc_lists_per_word: Dict mapping word -> [(doc_id, hit_list), ...]
    
    Returns:
        List of (score, doc_id) tuples sorted by score descending
    """
    if len(query_words) == 1:
        word = query_words[0]
        return rank_single_query(doc_lists_per_word[word])
    else:
        return rank_multi_query(doc_lists_per_word, normalize=True)