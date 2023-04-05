from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
from neo4j import AsyncSession as Neo4jAsyncSession

from migration_service.services.migration import add_migration
from migration_service.dependencies import db_session, neo4j_session
from migration_service.schemas.migrations import MigrationIn


router = APIRouter()


@router.post('/add')
async def create_migration(
        migration_in: MigrationIn,
        session: SQLAlchemyAsyncSession = Depends(db_session),
        graph_session: Neo4jAsyncSession = Depends(neo4j_session)
):
    guid = await add_migration(migration_in, session, graph_session)
    return {'guid': guid}
