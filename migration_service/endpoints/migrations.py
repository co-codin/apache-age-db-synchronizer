from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
from neo4j import AsyncSession as Neo4jAsyncSession

from migration_service.crud.migration import add_migration, select_migration
from migration_service.services.migration import apply_migration
from migration_service.dependencies import db_session, neo4j_session
from migration_service.schemas.migrations import MigrationIn, MigrationOut


router = APIRouter(
    tags=["migrations"]
)


@router.post('/add')
async def create_migration(
        migration_in: MigrationIn,
        session: SQLAlchemyAsyncSession = Depends(db_session),
        graph_session: Neo4jAsyncSession = Depends(neo4j_session)
):
    guid = await add_migration(migration_in, session, graph_session)
    return {'guid': guid}


@router.get('/{migration_uuid}', response_model=MigrationOut)
async def get_migration(migration_uuid: str, session: SQLAlchemyAsyncSession = Depends(db_session)):
    migration_out = await select_migration(session, migration_uuid)
    return migration_out


@router.get('/', response_model=MigrationOut)
async def get_last_migration(session: SQLAlchemyAsyncSession = Depends(db_session)):
    migration_out = await select_migration(session)
    return migration_out


@router.post('/apply')
async def migrate(
        session: SQLAlchemyAsyncSession = Depends(db_session),
        graph_session: Neo4jAsyncSession = Depends(neo4j_session)
):
    await apply_migration(session, graph_session)
