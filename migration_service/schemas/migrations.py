from typing import List


from pydantic import BaseModel


class MigrationIn(BaseModel):
    name: str
    db_source: str


class FieldToCreate(BaseModel):
    name: str
    db_type: str


class TableToCreate(BaseModel):
    name: str
    fields: List[FieldToCreate] = []


class FieldToAlter(BaseModel):
    name: str
    old_type: str
    new_type: str


class TableToAlter(BaseModel):
    name: str
    fields_to_create: List[FieldToCreate] = []
    fields_to_alter: List[FieldToAlter] = []
    fields_to_delete: List[str] = []


class MigrationOut(BaseModel):
    name: str
    tables_to_create: List[TableToCreate] = []
    tables_to_alter: List[TableToAlter] = []
    tables_to_delete: List[str] = []
