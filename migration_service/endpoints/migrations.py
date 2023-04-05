from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
from neo4j import AsyncSession as Neo4jAsyncSession

from migration_service.services.migration import scan_db_for_migration
from migration_service.dependencies import db_session, neo4j_session


router = APIRouter()


@router.post('/scan')
async def scan_db(
        session: SQLAlchemyAsyncSession = Depends(db_session),
        graph_session: Neo4jAsyncSession = Depends(neo4j_session)
):
    await scan_db_for_migration(session, graph_session)
