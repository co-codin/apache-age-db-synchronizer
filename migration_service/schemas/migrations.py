from pydantic import BaseModel


class MigrationIn(BaseModel):
    name: str
    db_source: str
