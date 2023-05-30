from pydantic import BaseModel


class FieldToCreate(BaseModel):
    is_key: str | None
    name: str
    db_type: str


class FieldToAlter(BaseModel):
    is_key: str | None
    name: str
    old_type: str
    new_type: str
