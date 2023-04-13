import itertools
import re
import logging

from typing import List, Optional, Dict, Iterable

from pydantic import BaseModel

from migration_service.errors import MoreThanTwoFieldsMatchFKPattern
from migration_service.utils.migration_utils import match_fk_to_table

logger = logging.getLogger(__name__)


class MigrationIn(BaseModel):
    name: str
    db_source: str


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
    hub_prefix: str = r'.*'
    hub_pattern = f'{hub_prefix}_hub'

    pk_pattern = "hash_key"

    fk_table = f"^({hub_prefix})_?({hub_prefix})_?({hub_prefix})_sat$"
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
            if table_prefix and not self.main_link:
                table_name = match_fk_to_table(table_prefix, tables)
                if table_name:
                    self.main_link = OneWayLink(ref_table=table_name, fk=field.name)
            elif table_prefix and self.main_link and not self.paired_link:
                table_name = match_fk_to_table(table_prefix, tables)
                if table_name:
                    self.paired_link = OneWayLink(ref_table=table_name, fk=field.name)
            elif not table_prefix:
                continue
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
