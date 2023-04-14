import re
import logging

from typing import Iterable, Optional

from migration_service.models.migrations import Migration, Table, Field
from migration_service.schemas.migrations import (
    ApplyMigration, FieldToCreate, HubToCreate, LinkToCreate, TableToCreate, SatToCreate, OneWayLink, TableToAlter,
    FieldToAlter
)

logger = logging.getLogger(__name__)


def format_orm_migration(migration: Migration, fk_pattern: str, pk_pattern: str) -> ApplyMigration:
    # Convert from ORM view to pydantic view
    apply_migration = ApplyMigration(db_source=migration.db_source)

    fk_pattern_compiled = re.compile(fk_pattern)
    pk_pattern_compiled = re.compile(pk_pattern)

    for table in migration.tables:
        if table.old_name is None and table.new_name is not None:
            # table to create
            _format_table_to_create(table, apply_migration, fk_pattern_compiled, pk_pattern_compiled)
        elif table.old_name is not None and table.new_name is None:
            # table to delete
            _format_table_to_delete(table, apply_migration, fk_pattern_compiled)
        elif table.old_name is not None and table.old_name == table.new_name:
            # table to alter
            table_to_alter = _format_table_to_alter(table)
            fk_count = table.fk_count(fk_pattern_compiled)

            if not fk_count:
                apply_migration.hubs_to_alter.append(table_to_alter)
            elif fk_count == 1:
                apply_migration.sats_to_alter.append(table_to_alter)
            elif fk_count == 2:
                apply_migration.links_to_alter.append(table_to_alter)
    return apply_migration


def _format_table_to_create(table: Table, migration: ApplyMigration, fk_pattern: re.Pattern, pk_pattern: re.Pattern):
    fk_count = table.fk_count(fk_pattern)
    logger.info(f"{table.new_name} fk count = {fk_count}")
    if not fk_count:
        hub = HubToCreate(name=table.new_name)
        migration.hubs_to_create.append(hub)
        _add_fields(hub, table.fields, pk_pattern=pk_pattern)
    elif fk_count == 1:
        sat = SatToCreate(name=table.new_name)
        migration.sats_to_create.append(sat)
        _add_fields(sat, table.fields, pk_pattern=pk_pattern, fk_pattern=fk_pattern)
    elif fk_count == 2:
        link = LinkToCreate(name=table.new_name)
        migration.links_to_create.append(link)
        _add_fields(link, table.fields, pk_pattern=pk_pattern)
    else:
        return


def _format_table_to_delete(table: Table, migration: ApplyMigration, fk_pattern: re.Pattern):
    fk_count = table.fk_count(fk_pattern)
    if not fk_count:
        migration.hubs_to_delete.append(table.old_name)
    elif fk_count == 1:
        migration.sats_to_delete.append(table.old_name)
    elif table.fk_count(fk_pattern) == 2:
        migration.links_to_create.append(table.old_name)
    else:
        return


def _add_fields(
        table: TableToCreate,
        fields: Iterable[Field],
        pk_pattern: Optional[re.Pattern] = None,
        fk_pattern: Optional[re.Pattern] = None
):
    possible_pk = None
    pk_count = 0

    possible_fk = None
    fk_count = 0

    for field in fields:
        field_to_create = FieldToCreate(name=field.new_name, db_type=field.new_type)
        table.fields.append(field_to_create)
        if pk_pattern and pk_pattern.search(field_to_create.name):
            possible_pk = field_to_create.name
            pk_count += 1
        if fk_pattern and fk_pattern.search(field_to_create.name):
            possible_fk = field_to_create.name
            fk_count += 1
    if pk_count == 1:
        table.pk = possible_pk
    if fk_count == 1:
        table.link = OneWayLink(fk=possible_fk)


def _format_table_to_alter(table: Table) -> TableToAlter:
    table_to_alter = TableToAlter(name=table.new_name)
    for field in table.fields:
        if field.old_name is None and field.new_name is not None:
            # Field to create
            table_to_alter.fields_to_create.append(
                FieldToCreate(name=field.new_name, db_type=field.new_type)
            )
        elif field.old_name is not None and field.new_name is None:
            # Field to delete
            table_to_alter.fields_to_delete.append(field.old_name)
        elif field.old_name is not None and field.old_name == field.new_name:
            # Field to alter
            table_to_alter.fields_to_alter.append(
                FieldToAlter(
                    name=field.new_name, old_type=field.old_type, new_type=field.new_type
                )
            )
    return table_to_alter
