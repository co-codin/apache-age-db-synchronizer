import json
import logging
import asyncio

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse


from migration_service.crud.migration import add_migration, select_migration
from migration_service.endpoints.migrations import router

from migration_service.schemas.migrations import MigrationIn, MigrationPattern
from migration_service.services.auth import load_jwks

from migration_service.database import db_session
from migration_service.database import ag_session

from migration_service.mq import create_channel, PikaChannel
from migration_service.errors import APIError
from migration_service.services.migration import apply_migration
from migration_service.settings import settings

logger = logging.getLogger(__name__)

migration_app = FastAPI(
    title="Graph DB migrater",
    description="Migration service for graph database"
)

migration_app.include_router(router)


@migration_app.on_event('startup')
async def on_startup():
    await load_jwks()

    async with create_channel() as channel:
        await channel.exchange_declare(settings.migration_exchange, 'direct')

        await channel.queue_declare(settings.migration_request_queue)
        await channel.queue_bind(settings.migration_request_queue, settings.migration_exchange, 'task')

        await channel.queue_declare(settings.migrations_result_queue)
        await channel.queue_bind(settings.migrations_result_queue, settings.migration_exchange, 'result')

        asyncio.create_task(consume(settings.migration_request_queue, synchronize))


@migration_app.middleware("http")
async def request_log(request: Request, call_next):
    try:
        response: Response = await call_next(request)
        if response.status_code < 400:
            logger.info(f"{request.method} {request.url} Status code: {response.status_code}")
        else:
            logger.warning(f"{request.method} {request.url} Status code: {response.status_code}")
        return response
    except Exception as exc:  # noqa
        logger.exception(str(exc))
        return JSONResponse(
            status_code=500,
            content={"message": "Something went wrong!"},
        )


@migration_app.exception_handler(APIError)
def api_exception_handler(_request: Request, exc: APIError) -> JSONResponse:
    logger.warning(str(exc))
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": str(exc)},
    )


@migration_app.get('/ping')
def ping():
    return {'status': 'ok'}


async def consume(query, func):
    while True:
        try:
            logger.info(f'Starting {query} worker')
            async with create_channel() as channel:
                async for body in channel.consume(query):
                    try:
                        await func(body, channel)
                    except Exception as e:
                        logger.exception(f'Failed to process message {body}: {e}')
        except Exception as e:
            logger.exception(f'Worker {query} failed: {e}')

        await asyncio.sleep(0.5)


async def synchronize(migration_request: str, channel: PikaChannel):
    migration_request = json.loads(migration_request)

    migration_in = MigrationIn(
        **{'name': migration_request['name'], 'conn_string': migration_request['conn_string']}
    )
    migration_pattern = MigrationPattern(**migration_request['migration_pattern'])
    source_registry_guid = migration_request['source_registry_guid']

    async with db_session() as session:
        with ag_session() as age_session:
            guid = await add_migration(migration_in, session, age_session)
            await apply_migration(migration_pattern, session, age_session)
            graph_migration = await select_migration(session, guid)

            await channel.basic_publish(
                exchange=settings.migration_exchange,
                routing_key='result',
                body=json.dumps(
                    {
                        'conn_string': migration_in.conn_string,
                        'graph_migration': graph_migration.dict(),
                        'source_registry_guid': source_registry_guid
                    }
                )
            )
