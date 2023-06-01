from pydantic import BaseModel


class FieldToCreate(BaseModel):
    is_key: bool | None
    name: str
    db_type: str


class FieldToAlter(BaseModel):
    is_key: bool | None
    name: str
    old_type: str
    new_type: str
