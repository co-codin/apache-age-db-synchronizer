from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession

from migration_service.crud.migration import select_migration
from migration_service.dependencies import db_session
from migration_service.schemas.migrations import MigrationOut


router = APIRouter(
    prefix='/migrations',
    tags=["migrations"]
)


@router.get('/{migration_guid}', response_model=MigrationOut)
async def get_migration(migration_guid: str, session: SQLAlchemyAsyncSession = Depends(db_session)):
    migration_out = await select_migration(session, migration_guid)
    return migration_out


@router.get('/', response_model=MigrationOut)
async def get_last_migration(session: SQLAlchemyAsyncSession = Depends(db_session)):
    migration_out = await select_migration(session)
    return migration_out
