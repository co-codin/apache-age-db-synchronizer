from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
from neo4j import AsyncSession as Neo4jAsyncSession

from migration_service.services.migration import add_migration, select_last_migration
from migration_service.dependencies import db_session, neo4j_session
from migration_service.schemas.migrations import MigrationIn


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


@router.get('/{migration_uuid}')
async def get_migration(migration_uuid: str, session: SQLAlchemyAsyncSession = Depends(db_session)):
    ...


@router.get('/')
async def get_last_migration(session: SQLAlchemyAsyncSession = Depends(db_session)):
    name = await select_last_migration(session)
    return name
