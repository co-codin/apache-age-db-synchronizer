from pydantic import BaseModel


class FieldToCreate(BaseModel):
    name: str
    db_type: str


class FieldToAlter(BaseModel):
    name: str
    old_type: str
    new_type: str
