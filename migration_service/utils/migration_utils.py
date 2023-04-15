import difflib

from typing import Iterable, Optional


def to_batches(records: Iterable, size: int = 50):
    batches = []
    for rec in records:
        batches.append(rec)
        if len(batches) >= size:
            yield batches
            batches.clear()
    if batches:
        yield batches


def get_highest_table_similarity_score(ref_table: str, tables: Iterable[str], exclude_table: Optional[str]) -> str:
    scores = {}
    for table in tables:
        if exclude_table and table != exclude_table:
            score = difflib.SequenceMatcher(None, a=ref_table, b=table).ratio()
            scores[table] = score
    return max(scores, key=scores.get)
