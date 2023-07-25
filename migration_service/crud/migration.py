import logging
import uuid
import asyncio

from typing import Union, Set, List, Sequence

from age import Age
from fastapi import status, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession

from migration_service.models import migrations
from migration_service.schemas import tables
from migration_service.schemas.migrations import MigrationIn, MigrationOut
from migration_service.services.migration_formatter import MigrationOutFormatter
from migration_service.services.metadata_extractor import MetaDataExtractorFactory, MetadataExtractor

from migration_service.utils.graph_db_utils import get_graph_db_tables, get_graph_db_table_col_type, get_graph_db_table

logger = logging.getLogger(__name__)


async def add_migration(
        migration_in: MigrationIn,
        session: SQLAlchemyAsyncSession,
        age_session: Age
) -> str:
    logger.info('Adding migration...')
    metadata_extractor = MetaDataExtractorFactory.build(conn_string=migration_in.conn_string)
    loop = asyncio.get_running_loop()

    if migration_in.object_name or migration_in.object_db_path:
        db_ns_to_table = await metadata_extractor.extract_table_name(
            table_name=migration_in.object_name, db_path=migration_in.object_db_path
        )
        graph_db_ns_to_table = await loop.run_in_executor(
            None, get_graph_db_table, db_ns_to_table.keys(), migration_in.object_name, age_session
        )
    else:
        db_ns_to_table = await metadata_extractor.extract_table_names()
        graph_db_ns_to_table = await loop.run_in_executor(
            None, get_graph_db_tables, db_ns_to_table.keys(), age_session
        )

    guid = str(uuid.uuid4())
    db_source = migration_in.conn_string.rsplit('/', maxsplit=1)[1]
    migration = migrations.Migration(name=migration_in.name, guid=guid, db_source=db_source)

    last_migration = await _select_last_migration_by_db_source(db_source, session)
    if last_migration is not None:
        logger.info(f"last migration name: {last_migration.name}")
        logger.info(f"last migration created_at: {last_migration.created_at}")
        migration.prev_migration = last_migration

    for ns, db_tables in db_ns_to_table.items():
        tables_to_delete = graph_db_ns_to_table[ns] - db_tables
        tables_to_create = db_tables - graph_db_ns_to_table[ns]
        tables_to_alter = graph_db_ns_to_table[ns] & db_tables

        logger.info(f'ns: {ns}')

        schema_name = ns.rsplit('.', maxsplit=1)[1]
        schema = migrations.Schema(name=schema_name, migration_guid=guid)

        await _create_tables(tables_to_create, metadata_extractor, schema)
        await _alter_tables(tables_to_alter, metadata_extractor, schema, db_source, age_session)
        await _delete_tables(tables_to_delete, schema)

        migration.schemas.append(schema)

    session.add(migration)
    await session.commit()
    return migration.guid


async def select_migration(session: SQLAlchemyAsyncSession, guid: str = None) -> MigrationOut:
    logger.info('Selecting migration...')
    if guid:
        migration = await _select_migration_tables_fields_by_guid(guid, session)
    else:
        migration = await select_last_migration_tables_fields(session)

    if migration is not None:
        logger.info(f"migration name: {migration.name}")
        formatted_migration = MigrationOutFormatter(migration).format()
        return formatted_migration
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


async def _create_tables(
        table_names: Set[str], metadata_extractor: MetadataExtractor, schema: migrations.Schema,
):
    if not table_names:
        return

    records = await metadata_extractor.extract_table_col_type(table_names, schema.name)
    dataclass_db_tables = _create_dataclass_tables(records)

    for db_table in dataclass_db_tables:
        table = migrations.Table(new_name=db_table.name, db=db_table.db)
        for field_name, field_type in db_table.field_to_type.items():
            table.fields.append(
                migrations.Field(new_name=field_name, new_type=field_type)
            )
        schema.tables.append(table)


async def _alter_tables(
        table_names: Set[str], metadata_extractor: MetadataExtractor, schema: migrations.Schema, db_source: str,
        age_session: Age
):
    if not table_names:
        return
    db_records = await metadata_extractor.extract_table_col_type(table_names, schema.name)

    loop = asyncio.get_running_loop()
    graph_db_records = await loop.run_in_executor(
        None, get_graph_db_table_col_type, db_source, schema.name, table_names, age_session
    )

    dataclass_db_tables = _create_dataclass_tables(db_records)
    dataclass_graph_db_tables = _create_dataclass_tables(graph_db_records)

    _do_tables_altering(dataclass_db_tables, dataclass_graph_db_tables, schema)


async def _delete_tables(table_names: Set[str], schema: migrations.Schema):
    for table in table_names:
        schema.tables.append(migrations.Table(old_name=table, db=f'{schema.name}.{table}'))


def _do_tables_altering(
        dataclass_db_tables: List[tables.Table],
        dataclass_graph_db_tables: List[tables.Table],
        schema: migrations.Schema
):
    for db_table, graph_db_table in zip(dataclass_db_tables, dataclass_graph_db_tables):
        assert db_table.name == graph_db_table.name
        if db_table == graph_db_table:
            continue
        else:
            table = migrations.Table(
                old_name=graph_db_table.name,
                new_name=graph_db_table.name,
                db=graph_db_table.db
            )

            schema.tables.append(table)

            db_table_field_names = set(db_table.field_to_type.keys())
            graph_db_table_field_names = set(graph_db_table.field_to_type.keys())

            fields_to_delete = graph_db_table_field_names - db_table_field_names
            fields_to_create = db_table_field_names - graph_db_table_field_names
            fields_to_alter = graph_db_table_field_names & db_table_field_names

            _create_fields(fields_to_create, db_table, table)
            _alter_fields(fields_to_alter, db_table, graph_db_table, table)
            _delete_fields(fields_to_delete, graph_db_table, table)


def _create_fields(fields_to_create: Set[str], db_table: tables.Table, table: migrations.Table):
    for f_to_create in fields_to_create:
        field = migrations.Field(new_name=f_to_create, new_type=db_table.field_to_type[f_to_create])
        table.fields.append(field)


def _alter_fields(
        fields_to_alter: Set[str], db_table: tables.Table, neo4j_table: tables.Table, table: migrations.Table
):
    for f_to_alter in fields_to_alter:
        db_type = db_table.field_to_type[f_to_alter]
        graph_db_type = neo4j_table.field_to_type[f_to_alter]
        if db_type == graph_db_type:
            continue
        else:
            field = migrations.Field(old_name=f_to_alter, new_name=f_to_alter, old_type=graph_db_type, new_type=db_type)
            table.fields.append(field)


def _delete_fields(fields_to_delete: Set[str], neo4j_table: tables.Table, table: migrations.Table):
    for f_to_delete in fields_to_delete:
        field = migrations.Field(old_name=f_to_delete, old_type=neo4j_table.field_to_type[f_to_delete])
        table.fields.append(field)


async def _select_migration_tables_fields_by_guid(guid: str, session: SQLAlchemyAsyncSession):
    migration = await session.execute(
        select(migrations.Migration)
        .options(
            selectinload(migrations.Migration.schemas)
            .selectinload(migrations.Schema.tables)
            .selectinload(migrations.Table.fields)
        )
        .where(migrations.Migration.guid == guid)
    )
    return migration.scalars().first()


async def select_last_migration_tables_fields(session: SQLAlchemyAsyncSession):
    migration = await session.execute(
        select(migrations.Migration)
        .options(
            selectinload(migrations.Migration.schemas)
            .selectinload(migrations.Schema.tables)
            .selectinload(migrations.Table.fields)
        )
        .order_by(migrations.Migration.created_at.desc())
        .limit(1)
    )
    return migration.scalars().first()


async def _select_last_migration(session: SQLAlchemyAsyncSession) -> Union[migrations.Migration, None]:
    last_migration = await session.execute(
        select(migrations.Migration)
        .order_by(migrations.Migration.created_at.desc())
        .limit(1)
    )
    return last_migration.scalars().first()


async def _select_last_migration_by_db_source(db_source: str, session: SQLAlchemyAsyncSession) -> Union[migrations.Migration, None]:
    last_migration = await session.execute(
        select(migrations.Migration)
        .where(migrations.Migration.db_source == db_source)
        .order_by(migrations.Migration.created_at.desc())
        .limit(1)
    )
    return last_migration.scalars().first()


def _create_dataclass_tables(db_records: Sequence[Sequence[str]]) -> List[tables.Table]:
    db_tables: List[tables.Table] = []
    if not db_records:
        return db_tables

    db = db_records[0][0]
    name = db_records[0][1]
    db_tables.append(tables.Table(db=db, name=name))

    for record in db_records:
        if name != record[1]:
            db = record[0]
            name = record[1]
            db_tables.append(tables.Table(db=db, name=name))

        field_name = record[2]
        field_type = record[3]
        db_tables[-1].field_to_type[field_name] = field_type
    return db_tables
