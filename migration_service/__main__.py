# type: ignore[attr-defined]
import uvicorn

from migration_service.logger_config import config_logger
from migration_service.settings import settings
from migration_service.app import migration_app

config_logger()

if __name__ == "__main__":
    uvicorn.run(migration_app, host="0.0.0.0", port=settings.port)
