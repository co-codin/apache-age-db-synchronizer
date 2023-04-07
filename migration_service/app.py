import logging

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from migration_service.endpoints.migrations import router
from migration_service.services.auth import load_jwks
from migration_service.errors import APIError

logger = logging.getLogger(__name__)

migration_app = FastAPI(
    title="Graph DB migrater",
    description="Migration service for graph database"
)

migration_app.include_router(router, prefix='/migrations')


@migration_app.on_event('startup')
async def on_startup():
    await load_jwks()


@migration_app.middleware("http")
async def request_log(request: Request, call_next):
    """
    Global exception handler for catching non API errors.
    ALso catch, sort and write uvicorn output and critical errors to log
    :param request: Request
    :param call_next: call_next
    :return: JSONResponse
    """
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
    """
    Exception handler for catching API errors
    :param _request: Request
    :param exc: APIError
    :return: JSONResponse
    """
    logger.warning(str(exc))
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": str(exc)},
    )


@migration_app.get('/ping')
def ping():
    return {'status': 'ok'}
