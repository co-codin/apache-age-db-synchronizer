from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer

from neo4j import AsyncGraphDatabase
from neo4j.exceptions import ServiceUnavailable

from migration_service.database import db_session as _db_session
from migration_service.services.auth import decode_jwt
from migration_service.settings import settings
from migration_service.errors import NoNeo4jConnection


bearer = HTTPBearer()
driver = AsyncGraphDatabase.driver(settings.neo4j_connection_string, auth=settings.neo4j_auth)


async def db_session():
    async with _db_session() as session:
        yield session


async def get_user(token=Depends(bearer)) -> dict:
    try:
        return await decode_jwt(token.credentials)
    except Exception:
        raise HTTPException(status_code=401)


async def neo4j_session():
    async with driver.session() as session:
        try:
            yield session
        except (ServiceUnavailable, ValueError):
            raise NoNeo4jConnection(settings.neo4j_connection_string)
