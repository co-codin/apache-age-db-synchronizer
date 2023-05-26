import age

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer

from migration_service.database import db_session as _db_session
from migration_service.services.auth import decode_jwt
from migration_service.settings import settings


bearer = HTTPBearer()
ag = age.connect(dsn=settings.age_connection_string)


async def db_session():
    async with _db_session() as session:
        yield session


async def get_user(token=Depends(bearer)) -> dict:
    try:
        return await decode_jwt(token.credentials)
    except Exception:
        raise HTTPException(status_code=401)


def ag_session():
    try:
        yield ag
        ag.commit()
    except Exception as exc:
        ag.rollback()
        raise exc
