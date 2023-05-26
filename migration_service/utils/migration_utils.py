import difflib

from typing import Iterable, Optional


def to_batches(records: Iterable, size: int = 50, fields_key: str = 'fields'):
    batches = []
    for rec in records:

        fields = rec[fields_key]
        fields_len = len(fields)

        for ndx in range(0, fields_len, size):
            field_batch = rec[fields_key][ndx:min(ndx + size, fields_len)]
            item = {**rec, fields_key: field_batch}

            batches.append(item)

        if len(batches) >= size:
            yield batches
            batches.clear()
    if batches:
        yield batches


def get_highest_table_similarity_score(ref_table: str, tables: Iterable[str], exclude_table: str) -> Optional[str]:
    table_to_score = {
        table: difflib.SequenceMatcher(None, a=ref_table, b=table).ratio()
        for table in tables
        if table != exclude_table
    }
    if table_to_score:
        table_to_score_sorted = sorted(table_to_score.items(), key=lambda x: x[1], reverse=True)
        score = table_to_score_sorted[0][1]
        min_score = 0.65
        if score >= min_score:
            return table_to_score_sorted[0][0]
