from age import Age
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession

from migration_service.crud.migration import add_migration, select_migration
from migration_service.services.migration import apply_migration
from migration_service.dependencies import db_session, ag_session
from migration_service.schemas.migrations import MigrationIn, MigrationOut, MigrationPattern


router = APIRouter(
    tags=["migrations"]
)


@router.post('/add')
async def create_migration(
        migration_in: MigrationIn,
        session: SQLAlchemyAsyncSession = Depends(db_session),
        age_session: Age = Depends(ag_session)
):
    guid = await add_migration(migration_in, session, age_session)
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
        migration_pattern: MigrationPattern,
        session: SQLAlchemyAsyncSession = Depends(db_session),
        age_session: Age = Depends(ag_session)
):
    guid = await apply_migration(migration_pattern, session, age_session)
    return {'message': f'migration with guid {guid} has been applied'}
