import logging
import asyncio

from typing import Callable

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse


from migration_service.endpoints.migrations import router

from migration_service.services.auth import load_jwks
from migration_service.services.migration_request_lifespan import synchronize, set_synchronizing_off

from migration_service.mq import create_channel
from migration_service.errors import APIError
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

        asyncio.create_task(consume(settings.migration_request_queue, synchronize, set_synchronizing_off))


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


async def consume(query, func: Callable, reject_func: Callable = None):
    while True:
        try:
            logger.info(f'Starting {query} worker')
            async with create_channel() as channel:
                async for delivery_tag, body in channel.consume(query):
                    try:
                        logger.info(f"Received message: {body}")
                        await func(body, channel)
                        await channel.basic_ack(delivery_tag)
                    except Exception as e:
                        logger.exception(f'Failed to process message {body}: {e}')
                        await channel.basic_reject(delivery_tag, requeue=False)

                        if reject_func:
                            await reject_func(body, channel)
        except Exception as e:
            logger.exception(f'Worker {query} failed: {e}')

        await asyncio.sleep(0.5)
