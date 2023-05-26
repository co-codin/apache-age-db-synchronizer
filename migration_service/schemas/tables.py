import re

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Iterable

from pydantic import BaseModel

from migration_service.schemas.fields import FieldToCreate, FieldToAlter
from migration_service.utils.migration_utils import get_highest_table_similarity_score
from migration_service.errors import MoreThanTwoFieldsMatchFKPattern


@dataclass(frozen=True)
class Table:
    name: str
    db: str
    field_to_type: Dict[str, str] = field(default_factory=dict)

    def __hash__(self):
        return hash((self.name, self.db, self.field_to_type))


class TableToCreate(BaseModel):
    name: str
    db: str
    fields: List[FieldToCreate] = []

    @property
    def field_name_set(self):
        return {field.name for field in self.fields}


class TableToAlter(BaseModel):
    name: str
    fields_to_create: List[FieldToCreate] = []
    fields_to_alter: List[FieldToAlter] = []
    fields_to_delete: List[str] = []


class HubToCreate(TableToCreate):
    pk: Optional[str]


class OneWayLink(BaseModel):
    ref_table_pk: Optional[str]
    fk: str
    ref_table: Optional[str]


class SatToCreate(TableToCreate):
    link: Optional[OneWayLink]
    pk: Optional[str]


class LinkToCreate(TableToCreate):
    main_link: Optional[OneWayLink]
    paired_link: Optional[OneWayLink]

    pk: Optional[str]

    def match_fks_to_fk_tables(self, fk_pattern: re.Pattern, tables: Iterable[str]):
        for field in self.fields:
            table_prefix = fk_pattern.search(field.name)
            if not table_prefix:
                continue
            elif table_prefix and not self.main_link:
                table_name = get_highest_table_similarity_score(table_prefix.group(1), tables, self.name)
                if table_name:
                    self.main_link = OneWayLink(ref_table=table_name, fk=field.name)
            elif table_prefix and self.main_link and not self.paired_link:
                table_name = get_highest_table_similarity_score(table_prefix.group(1), tables, self.name)
                if table_name:
                    self.paired_link = OneWayLink(ref_table=table_name, fk=field.name)
            else:
                raise MoreThanTwoFieldsMatchFKPattern(
                    (self.main_link.fk, self.paired_link.fk, field.name),
                    fk_pattern.pattern
                )
