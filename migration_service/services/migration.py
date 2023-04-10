import logging
import uuid
import psycopg

from typing import Set, List, Sequence, Union, Iterable

from fastapi import status, HTTPException
from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
from neo4j import AsyncSession as Neo4jAsyncSession, AsyncManagedTransaction

from migration_service.models import migrations
from migration_service.schemas import tables
from migration_service.schemas.migrations import (
    MigrationIn, MigrationOut, TableToCreate, FieldToCreate, TableToAlter, FieldToAlter
)
from migration_service.settings import settings
from migration_service.cql_queries.node_queries import delete_nodes_query


logger = logging.getLogger(__name__)


async def add_migration(
        migration_in: MigrationIn, session: SQLAlchemyAsyncSession, graph_session: Neo4jAsyncSession
) -> str:
    db_tables = await _get_db_tables(migration_in.db_source)
    neo4j_tables = await _get_neo4j_tables(graph_session, migration_in.db_source)

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

    last_migration = await _get_last_migration(session)
    if last_migration is not None:
        logger.info(f"last migration name: {last_migration.name}")
        logger.info(f"last migration created_at: {last_migration.created_at}")
        migration.prev_migration = last_migration

    session.add(migration)
    await session.commit()
    return guid


async def select_migration(session: SQLAlchemyAsyncSession, guid: str = None) -> MigrationOut:
    if guid:
        migration = await _select_migration_by_guid(guid, session)
    else:
        migration = await _select_last_migration(session)

    if migration is not None:
        logger.info(f"migration name: {migration.name}")
        logger.info(f"migration created_at: {migration.created_at}")
        formatted_migration = _format_orm_migration(migration)
        return formatted_migration
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)


async def apply_migration(
        session: SQLAlchemyAsyncSession, graph_session: Neo4jAsyncSession
):
    """
    1) Insert tables
    2) Alter tables
    3) Delete tables
    """
    last_migration_id = await _select_last_migration_id(session)
    await _apply_delete_tables(last_migration_id, session, graph_session)


async def _select_last_migration_id(session: SQLAlchemyAsyncSession):
    last_migration_id = await session.execute(
        select(migrations.Migration.id)
        .order_by(migrations.Migration.created_at.desc())
        .limit(1)
    )
    return last_migration_id.scalars().first()


async def _apply_create_tables(last_migration_id: int, session: SQLAlchemyAsyncSession, graph_session: Neo4jAsyncSession):
    tables_to_create = await session.execute(
        select(migrations.Table)
        .options(selectinload(migrations.Table.fields))
        .filter(
            and_(
                migrations.Table.migration_id == last_migration_id,
                migrations.Table.old_name.is_(None),
                migrations.Table.new_name.is_not(None)
            )
        )
    )
    tables_to_create = tables_to_create.scalars().all()
    return tables_to_create


async def _apply_alter_tables(last_migration_id: int, session: SQLAlchemyAsyncSession, graph_session: Neo4jAsyncSession):
    tables_to_alter = await session.execute(
        select(migrations.Table)
        .options(selectinload(migrations.Table.fields))
        .filter(
            and_(
                migrations.Table.migration_id == last_migration_id,
                migrations.Table.old_name.is_not(None),
                migrations.Table.old_name == migrations.Table.new_name
            )
        )
    )
    tables_to_alter = tables_to_alter.scalars().all()
    return tables_to_alter


async def _apply_delete_tables(last_migration_id: int, session: SQLAlchemyAsyncSession, graph_session: Neo4jAsyncSession):
    tables_to_delete = await session.execute(
        select(migrations.Table.old_name)
        .filter(
            and_(
                migrations.Table.migration_id == last_migration_id,
                migrations.Table.old_name.is_not(None),
                migrations.Table.new_name.is_(None)
            )
        )
    )
    tables_to_delete = tables_to_delete.scalars().all()
    await graph_session.execute_write(_delete_nodes_tx, tables_to_delete)


async def _delete_nodes_tx(tx: AsyncManagedTransaction, nodes_to_delete: Iterable[str]):
    for node_batch in _to_batches(nodes_to_delete):
        logger.info(f'nodes_batch: {node_batch}')
        await tx.run(delete_nodes_query, node_names=node_batch)


def _to_batches(records: Iterable[str], size: int = 50):
    batches = []
    for rec in records:
        batches.append(rec)
        if len(batches) >= size:
            yield batches
            batches.clear()
    if batches:
        yield batches


async def _select_migration_by_guid(guid: str, session: SQLAlchemyAsyncSession):
    migration = await session.execute(
        select(migrations.Migration)
        .options(selectinload(migrations.Migration.tables).selectinload(migrations.Table.fields))
        .filter(migrations.Migration.guid == guid)
    )
    return migration.scalars().first()


async def _select_last_migration(session: SQLAlchemyAsyncSession):
    migration = await session.execute(
        select(migrations.Migration)
        .options(selectinload(migrations.Migration.tables).selectinload(migrations.Table.fields))
        .order_by(migrations.Migration.created_at.desc())
        .limit(1)
    )
    return migration.scalars().first()


def _format_orm_migration(migration: migrations.Migration) -> MigrationOut:
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


async def _get_last_migration(session: SQLAlchemyAsyncSession) -> Union[migrations.Migration, None]:
    last_migration = await session.execute(
        select(migrations.Migration).order_by(migrations.Migration.created_at.desc()).limit(1)
    )
    return last_migration.scalars().first()


async def _get_db_tables(db_source: str) -> Set[str]:
    conn_string = settings.db_sources[db_source]
    async with await psycopg.AsyncConnection.connect(conn_string) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "select table_name "
                "from information_schema.tables "
                "where table_schema = %s;",
                (db_source,)
            )
            table_names = await cursor.fetchall()
            return {res[0] for res in table_names}


async def _get_neo4j_tables(graph_session: Neo4jAsyncSession, db_source: str) -> Set[str]:
    res = await graph_session.run(
        "MATCH (obj) "
        "WITH split(obj.db, $db_source + '.')[1] as db_name, obj "
        "WHERE (obj:Entity OR obj:Sat OR obj:Link) and obj.db STARTS WITH $db_source "
        "RETURN db_name as name;",
        db_source=db_source
    )
    table_names = await res.value('name')
    return set(table_names)


async def _create_tables(table_names: Set[str], migration: migrations.Migration, db_source: str):
    if not table_names:
        return

    records = await _get_table_col_type(table_names, db_source)
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
    db_records = await _get_table_col_type(table_names, db_source)
    neo4j_records = await _get_neo4j_table_col_type(table_names, db_source, graph_session)

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


def _create_dataclass_tables(db_records: Sequence[Sequence]) -> List[tables.Table]:
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


async def _get_table_col_type(table_names: Set[str], db_source: str):
    conn_string = settings.db_sources[db_source]
    async with await psycopg.AsyncConnection.connect(conn_string) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                "SELECT tabs.table_name, cols.column_name, cols.data_type "
                "FROM information_schema.columns AS cols "
                "JOIN information_schema.tables AS tabs "
                "ON tabs.table_name = cols.table_name "
                "WHERE tabs.table_schema = %s AND tabs.table_name = ANY(%s) "
                "ORDER BY tabs.table_name;",
                (db_source, list(table_names))
            )
            records = await cursor.fetchall()
    return records


async def _get_neo4j_table_col_type(table_names: Set[str], db_source: str, graph_session: Neo4jAsyncSession):
    res = await graph_session.run(
        "MATCH (obj)-[:ATTR]->(f:Field)  "
        "WITH split(obj.db, $db_source + '.')[1] AS db_name, obj, f "
        "WHERE (obj:Entity OR obj:Sat OR obj:Link) and obj.db STARTS WITH $db_source AND db_name IN $tables "
        "RETURN db_name as name, f.db as f_name, f.dbtype as f_type "
        "ORDER BY db_name;",
        db_source=db_source, tables=list(table_names)
    )
    table_names = await res.values('name', 'f_name', 'f_type')
    return table_names
