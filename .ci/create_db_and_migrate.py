from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from alembic.config import Config
from alembic import command
from migration_service.settings import settings


engine = create_engine(settings.db_migration_connection_string)

if not database_exists(engine.url):
    create_database(engine.url)

alembic_cfg = Config("alembic.ini")
command.upgrade(alembic_cfg, "head")
