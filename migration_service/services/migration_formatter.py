from migration_service.models import migrations
from migration_service.schemas.migrations import MigrationOut, TableToCreate, TableToAlter, FieldToCreate, FieldToAlter


def format_orm_migration(migration: migrations.Migration) -> MigrationOut:
    # Convert from ORM view to pydantic view
    migration_out = MigrationOut(name=migration.name)
    for table in migration.tables:
        if table.old_name is None and table.new_name is not None:
            # table to create
            table_to_create = _format_table_to_create(table)
            migration_out.tables_to_create.append(table_to_create)
        elif table.old_name is not None and table.new_name is None:
            # table to delete
            migration_out.tables_to_delete.append(table.old_name)
        elif table.old_name is not None and table.old_name == table.new_name:
            # table to alter
            table_to_alter = _format_table_to_alter(table)
            migration_out.tables_to_alter.append(table_to_alter)
    return migration_out


def _format_table_to_create(table: migrations.Table) -> TableToCreate:
    table_to_create = TableToCreate(name=table.new_name)
    for field in table.fields:
        field_to_create = FieldToCreate(name=field.new_name, db_type=field.new_type)
        table_to_create.fields.append(field_to_create)
    return table_to_create


def _format_table_to_alter(table: migrations.Table) -> TableToAlter:
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
