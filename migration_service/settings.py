from collections import namedtuple

from pydantic import BaseSettings

import os

Neo4jCreds = namedtuple('Neo4jCreds', ['username', 'password'])


class Settings(BaseSettings):
    # Uvicorn constants
    port: int = 8081

    # Logging constants
    debug = False
    log_dir: str = "/var/log/n3dwh/"
    log_name: str = "graph_db_migrater.log"

    db_sources: dict = {
        'dv_raw': os.environ.get('dwh_graph_db_migrater_db_source', 'postgresql://postgres:dwh@db.lan:5432/dwh')
    }

    # Database constants
    db_connection_string: str = 'postgresql+asyncpg://postgres:dwh@db.lan:5432/graph_migrations'
    db_migration_connection_string: str = 'postgresql+psycopg2://postgres:dwh@db.lan:5432/graph_migrations'

    # Neo4j constants
    neo4j_connection_string: str = 'bolt://graphdb.lan:7687'
    neo4j_auth: Neo4jCreds = (os.environ.get('dwh_graph_db_migrater_neo4j_connection_user', 'neo4j'), os.environ.get('dwh_graph_db_migrater_neo4j_connection_password', 'dwh'))

    # Service's urls
    api_iam = 'http://iam.lan:8000'

    class Config:
        env_prefix = "dwh_graph_db_migrater_"
        case_sensitive = False


settings = Settings()
