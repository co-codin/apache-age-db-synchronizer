import os
import logging
import ecs_logging

from logging.handlers import TimedRotatingFileHandler

from migration_service.settings import settings


def config_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if settings.debug else logging.INFO)
    os.makedirs(settings.log_dir, exist_ok=True)

    stream_handler = logging.StreamHandler()
    logger.addHandler(stream_handler)

    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(settings.log_dir, settings.log_name),
        when='D', backupCount=2, encoding='utf-8'
    )
    file_handler.setFormatter(ecs_logging.StdlibFormatter())
    logger.addHandler(file_handler)
