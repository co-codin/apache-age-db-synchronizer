# type: ignore[attr-defined]
import os
import uvicorn

from migration_service.logger_config import config_logger


config_logger()


def main() -> None:
    port = int(os.environ.get("PORT", 8081))
    reload = bool(os.environ.get("RELOAD", True))
    uvicorn.run("migration_service.app:migration_app", host="0.0.0.0", port=port, reload=reload)


if __name__ == "__main__":
    main()
