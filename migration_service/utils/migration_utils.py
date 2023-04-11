from typing import Iterable, Sequence, List

from migration_service.schemas import tables


def create_dataclass_tables(db_records: Sequence[Sequence]) -> List[tables.Table]:
    db_tables: List[tables.Table] = []
    if not db_records:
        return db_tables

    table_d = db_records[0][0]
    db_tables.append(tables.Table(name=table_d))

    for record in db_records:
        if table_d == record[0]:
            field_name = record[1]
            field_type = record[2]
            db_tables[-1].field_to_type[field_name] = field_type
        else:
            table_d = record[0]
            db_tables.append(tables.Table(name=table_d))
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
