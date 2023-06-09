from pydantic import BaseModel


class FieldToCreate(BaseModel):
    is_key: bool | None
    name: str
    db_type: str | None


class FieldToAlter(BaseModel):
    is_key: bool | None
    name: str
    old_type: str | None
    new_type: str | None
