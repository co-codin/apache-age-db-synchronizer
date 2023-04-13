import re
from typing import Iterable, Sequence, List, Optional

from migration_service.schemas import tables


def create_dataclass_tables(db_records: Sequence[Sequence]) -> List[tables.Table]:
    db_tables: List[tables.Table] = []
    if not db_records:
        return db_tables

    table_d = db_records[0][0]
    db_tables.append(tables.Table(name=table_d))

    for record in db_records:
        if table_d != record[0]:
            table_d = record[0]
            db_tables.append(tables.Table(name=table_d))

        field_name = record[1]
        field_type = record[2]
        db_tables[-1].field_to_type[field_name] = field_type
    return db_tables


def to_batches(records: Iterable, size: int = 50):
    batches = []
    for rec in records:
        batches.append(rec)
        if len(batches) >= size:
            yield batches
            batches.clear()
    if batches:
        yield batches


def get_table_name_by_prefix(table_prefix: str, table_names: Iterable[str]) -> Optional[str]:
    if not table_prefix:
        return

    count = 0
    possible_table_name = None
    for table in table_names:
        if table_prefix in table:
            count += 1
            possible_table_name = table
    if count == 1:
        return possible_table_name


def match_fk_to_table(table_prefix: re.Match, tables: Iterable[str]) -> Optional[str]:
    if not table_prefix:
        return

    for i in range(1, len(table_prefix.groups()) + 1):
        table_name = get_table_name_by_prefix(table_prefix.group(i), tables)
        if table_name:
            return table_name
