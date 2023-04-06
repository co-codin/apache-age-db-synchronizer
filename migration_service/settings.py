from collections import namedtuple

from pydantic import BaseSettings

Neo4jCreds = namedtuple('Neo4jCreds', ['username', 'password'])


class Settings(BaseSettings):
    # Uvicorn constants
    port: int = 8081

    # Logging constants
    debug = False
    log_dir: str = "/var/log/n3dwh/"
    log_name: str = "graph_db_migrater.log"

    db_sources: dict = {
        'dv_raw': 'postgresql://postgres:dwh@db.lan:5432/dwh'
    }

    # Database constants
    db_connection_string: str = 'postgresql+asyncpg://postgres:dwh@db.lan:5432/graph_migrations'
    db_migration_connection_string: str = 'postgresql+psycopg2://postgres:dwh@db.lan:5432/graph_migrations'

    # Neo4j constants
    neo4j_connection_string: str = 'bolt://graphdb.lan:7687'
    neo4j_auth: Neo4jCreds = ('neo4j', 'dwh')

    # Service's urls
    api_iam = 'http://iam.lan:8000'

    class Config:
        env_prefix = "dwh_graph_db_migrater_"
        case_sensitive = False


settings = Settings()
