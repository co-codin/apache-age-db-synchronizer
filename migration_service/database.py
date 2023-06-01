import age

from age import Age
from contextlib import asynccontextmanager, contextmanager

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from migration_service.settings import settings


engine = create_async_engine(
    settings.db_connection_string,
    echo=settings.debug
)


async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
Base = declarative_base()

ag = age.connect(dsn=settings.age_connection_string)


@asynccontextmanager
async def db_session() -> AsyncSession:
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@contextmanager
def ag_session() -> Age:
    try:
        yield ag
        ag.commit()
    except Exception as exc:
        ag.rollback()
        raise exc
