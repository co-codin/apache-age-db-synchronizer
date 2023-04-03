# type: ignore[attr-defined]
import os
import uvicorn

from migration_service.logger_config import config_logger


config_logger()


def main() -> None:
    from migration_service.app import migration_app
    app = migration_app
    port = int(os.environ.get("PORT", 8081))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
