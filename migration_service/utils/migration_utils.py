import difflib

from typing import Iterable, Optional, Sequence


def to_batches(records: Sequence, n: int = 50):
    iterable_len = len(records)
    for ndx in range(0, iterable_len, n):
        yield records[ndx:min(ndx + n, iterable_len)]


def get_highest_table_similarity_score(ref_table: str, tables: Iterable[str], exclude_table: str) -> Optional[str]:
    table_to_score = {
        table: difflib.SequenceMatcher(None, a=ref_table, b=table).ratio() * 100
        for table in tables
        if table != exclude_table
    }

    table_to_score_sorted = sorted(table_to_score.items(), key=lambda x: x[1], reverse=True)
    if table_to_score_sorted[0][1] != table_to_score_sorted[1][1]:
        return table_to_score_sorted[0][1]
