import difflib

from typing import Iterable, Optional


def alter_to_batches(records: Iterable, size: int = 100):
    batches = []
    for rec in records:
        for fields_key in ('fields_to_create', 'fields_to_alter', 'fields_to_delete'):
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


def delete_to_batches(records: Iterable, size: int = 100):
    batches = []
    for rec in records:
        batches.append(rec)

        if len(batches) >= size:
            yield batches
            batches.clear()
    if batches:
        yield batches


def add_to_batches(records: Iterable, size: int = 100):
    batches = []
    for rec in records:

        fields = rec['fields']
        fields_len = len(fields)

        for ndx in range(0, fields_len, size):
            field_batch = rec['fields'][ndx:min(ndx + size, fields_len)]
            item = {**rec, 'fields': field_batch}

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
        min_score = 0.7
        if score >= min_score:
            return table_to_score_sorted[0][0]
