import logging
import uuid

from typing import Union, Set, List, Sequence

from fastapi import status, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
from neo4j import AsyncSession as Neo4jAsyncSession

from migration_service.models import migrations
from migration_service.schemas import tables
from migration_service.schemas.migrations import MigrationIn, MigrationOut
from migration_service.services.migration_formatter import MigrationOutFormatter

from migration_service.utils.postgres_utils import get_db_tables, get_table_col_type
from migration_service.utils.neo4j_utils import get_neo4j_tables, get_neo4j_table_col_type

logger = logging.getLogger(__name__)


async def add_migration(
        migration_in: MigrationIn, session: SQLAlchemyAsyncSession, graph_session: Neo4jAsyncSession
) -> str:
    db_tables = await get_db_tables(migration_in.db_source)
    neo4j_tables = await get_neo4j_tables(graph_session, migration_in.db_source)

    logger.info(f"db tables: {db_tables}")
    logger.info(f"neo4j tables: {neo4j_tables}")

    tables_to_delete = neo4j_tables - db_tables
    tables_to_create = db_tables - neo4j_tables
    tables_to_alter = neo4j_tables & db_tables

    logger.info(f'tables to create: {tables_to_create}')
    logger.info(f'tables to alter: {tables_to_alter}')
    logger.info(f'tables to delete: {tables_to_delete}')

    guid = str(uuid.uuid4())
    migration = migrations.Migration(name=migration_in.name, guid=guid, db_source=migration_in.db_source)

    await _create_tables(tables_to_create, migration, migration_in.db_source)
    await _alter_tables(tables_to_alter, migration, migration_in.db_source, graph_session)
    await _delete_tables(tables_to_delete, migration)

    last_migration = await _select_last_migration(session)
    if last_migration is not None:
        logger.info(f"last migration name: {last_migration.name}")
        logger.info(f"last migration created_at: {last_migration.created_at}")
        migration.prev_migration = last_migration

    session.add(migration)
    await session.commit()
    return guid


async def select_migration(session: SQLAlchemyAsyncSession, guid: str = None) -> MigrationOut:
    if guid:
        migration = await _select_migration_tables_fields_by_guid(guid, session)
    else:
        migration = await select_last_migration_tables_fields(session)

    if migration is not None:
        logger.info(f"migration name: {migration.name}")
        logger.info(f"migration created_at: {migration.created_at}")
        formatted_migration = MigrationOutFormatter(migration).format()
        return formatted_migration
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


async def _create_tables(table_names: Set[str], migration: migrations.Migration, db_source: str):
    if not table_names:
        return

    records = await get_table_col_type(table_names, db_source)
    dataclass_db_tables = _create_dataclass_tables(records)

    for db_table in dataclass_db_tables:
        table = migrations.Table(new_name=db_table.name)
        for field_name, field_type in db_table.field_to_type.items():
            table.fields.append(
                migrations.Field(new_name=field_name, new_type=field_type)
            )
        migration.tables.append(table)


async def _alter_tables(
        table_names: Set[str], migration: migrations.Migration, db_source: str, graph_session: Neo4jAsyncSession
):
    if not table_names:
        return
    db_records = await get_table_col_type(table_names, db_source)
    neo4j_records = await get_neo4j_table_col_type(table_names, db_source, graph_session)

    logger.info(f'db records to alter: {db_records}')
    logger.info(f'neo4j records to alter: {neo4j_records}')

    dataclass_db_tables = _create_dataclass_tables(db_records)
    dataclass_neo4j_tables = _create_dataclass_tables(neo4j_records)

    logger.info(f'dataclass db tables to alter: {dataclass_db_tables}')
    logger.info(f'dataclass neo4j tables to alter: {dataclass_neo4j_tables}')

    _do_tables_altering(dataclass_db_tables, dataclass_neo4j_tables, migration)


async def _delete_tables(table_names: Set[str], migration: migrations.Migration):
    for table in table_names:
        migration.tables.append(migrations.Table(old_name=table))


def _do_tables_altering(
        dataclass_db_tables: List[tables.Table],
        dataclass_neo4j_tables: List[tables.Table],
        migration: migrations.Migration
):
    for db_table, neo4j_table in zip(dataclass_db_tables, dataclass_neo4j_tables):
        assert db_table.name == neo4j_table.name

        if db_table == neo4j_table:
            continue
        else:
            table = migrations.Table(old_name=neo4j_table.name, new_name=neo4j_table.name)

            migration.tables.append(table)

            db_table_field_names = set(db_table.field_to_type.keys())
            neo4j_table_field_names = set(neo4j_table.field_to_type.keys())

            fields_to_delete = neo4j_table_field_names - db_table_field_names
            fields_to_create = db_table_field_names - neo4j_table_field_names
            fields_to_alter = neo4j_table_field_names & db_table_field_names

            _create_fields(fields_to_create, db_table, table)
            _alter_fields(fields_to_alter, db_table, neo4j_table, table)
            _delete_fields(fields_to_delete, neo4j_table, table)


def _create_fields(fields_to_create: Set[str], db_table: tables.Table, table: migrations.Table):
    for f_to_create in fields_to_create:
        field = migrations.Field(new_name=f_to_create, new_type=db_table.field_to_type[f_to_create])
        table.fields.append(field)


def _alter_fields(
        fields_to_alter: Set[str], db_table: tables.Table, neo4j_table: tables.Table, table: migrations.Table
):
    for f_to_alter in fields_to_alter:
        db_type = db_table.field_to_type[f_to_alter]
        neo4j_type = neo4j_table.field_to_type[f_to_alter]
        if db_type == neo4j_type:
            continue
        else:
            field = migrations.Field(old_name=f_to_alter, new_name=f_to_alter, old_type=neo4j_type, new_type=db_type)
            table.fields.append(field)


def _delete_fields(fields_to_delete: Set[str], neo4j_table: tables.Table, table: migrations.Table):
    for f_to_delete in fields_to_delete:
        field = migrations.Field(old_name=f_to_delete, old_type=neo4j_table.field_to_type[f_to_delete])
        table.fields.append(field)


async def _select_last_migration_id(session: SQLAlchemyAsyncSession):
    last_migration_id = await session.execute(
        select(migrations.Migration.id)
        .order_by(migrations.Migration.created_at.desc())
        .limit(1)
    )
    return last_migration_id.scalars().first()


async def _select_migration_tables_fields_by_guid(guid: str, session: SQLAlchemyAsyncSession):
    migration = await session.execute(
        select(migrations.Migration)
        .options(selectinload(migrations.Migration.tables).selectinload(migrations.Table.fields))
        .filter(migrations.Migration.guid == guid)
    )
    return migration.scalars().first()


async def select_last_migration_tables_fields(session: SQLAlchemyAsyncSession):
    migration = await session.execute(
        select(migrations.Migration)
        .options(selectinload(migrations.Migration.tables).selectinload(migrations.Table.fields))
        .order_by(migrations.Migration.created_at.desc())
        .limit(1)
    )
    return migration.scalars().first()


async def _select_last_migration(session: SQLAlchemyAsyncSession) -> Union[migrations.Migration, None]:
    last_migration = await session.execute(
        select(migrations.Migration).order_by(migrations.Migration.created_at.desc()).limit(1)
    )
    return last_migration.scalars().first()


def _create_dataclass_tables(db_records: Sequence[Sequence]) -> List[tables.Table]:
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
