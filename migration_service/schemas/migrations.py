import itertools
import re
import logging

from typing import List, Optional, Dict, Iterable

from pydantic import BaseModel, validator

from migration_service.errors import MoreThanTwoFieldsMatchFKPattern, UnknownDBSource
from migration_service.utils.migration_utils import get_highest_table_similarity_score
from migration_service.settings import settings

logger = logging.getLogger(__name__)


class MigrationIn(BaseModel):
    name: str
    db_source: str

    @validator('db_source')
    def db_source_must_exist_in_settings(cls, v):
        if v not in settings.db_sources.keys():
            raise UnknownDBSource(v, settings.db_sources.keys())
        else:
            return v


class FieldToCreate(BaseModel):
    name: str
    db_type: str


class TableToCreate(BaseModel):
    name: str
    fields: List[FieldToCreate] = []

    @property
    def field_name_set(self):
        return {field.name for field in self.fields}


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


class MigrationPattern(BaseModel):
    hub_prefix: str = r'\w*'
    hub_pattern = f'{hub_prefix}_hub'

    pk_pattern = "hash_key"

    fk_table = f"^({hub_prefix})_sat$"
    fk_pattern = f"^(?:id)?({hub_prefix})_hash_fkey$"


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


class ApplyMigration(BaseModel):
    db_source: str

    hubs_to_create: List[HubToCreate] = []
    sats_to_create: List[SatToCreate] = []
    links_to_create: List[LinkToCreate] = []

    hubs_to_alter: List[TableToAlter] = []
    sats_to_alter: List[TableToAlter] = []
    links_to_alter: List[TableToAlter] = []

    hubs_to_delete: List[str] = []
    sats_to_delete: List[str] = []
    links_to_delete: List[str] = []

    @property
    def tables_to_pks(self) -> Dict[str, str]:
        return {
            table.name: table.pk
            for table in itertools.chain(self.hubs_to_create, self.sats_to_create, self.links_to_create)
            if table.pk
        }
