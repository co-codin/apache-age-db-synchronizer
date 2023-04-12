import logging
import re
import logging

from typing import List, Optional, Dict, Tuple

from pydantic import BaseModel

from migration_service.errors import MoreThanFieldsMatchFKPattern


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
    hub_pk_pattern = "hash_key"

    sat_pattern =f"^\w*_?{hub_prefix}_?\w*_sat$"
    fk_pattern = f"^(id)?({hub_prefix})_hash_fkey$"


class HubToCreate(TableToCreate):
    pk: Optional[str]


class OneWayLink(BaseModel):
    ref_table_pk: Optional[str]
    fk: str
    hub: str


class LinkToCreate(TableToCreate):
    main_link: Optional[OneWayLink]
    paired_link: Optional[OneWayLink]

    def match_link_fkeys(self, fk_pattern: re.Pattern, hub_names: Tuple[str]):
        for field in self.fields:
            hub_prefix = fk_pattern.search(field.name)
            if hub_prefix and not self.main_link:
                hub_name = self.get_hub_name(hub_prefix.group(2), hub_names)
                if hub_name:
                    self.main_link = OneWayLink(hub=hub_name, fk=field.name)
            elif hub_prefix and self.main_link and not self.paired_link:
                hub_name = self.get_hub_name(hub_prefix.group(2), hub_names)
                if hub_name:
                    self.paired_link = OneWayLink(hub=hub_name, fk=field.name)
            elif not hub_prefix:
                continue
            else:
                raise MoreThanFieldsMatchFKPattern()

    @staticmethod
    def get_hub_name(hub_prefix: str, hub_names: Tuple[str]):
        count = 0
        possible_hub_name = None
        hub_pattern = re.compile(hub_prefix)
        for hub in hub_names:
            if hub_pattern.search(hub):
                count += 1
                possible_hub_name = hub
        if count == 1:
            return possible_hub_name


class ApplyMigration(BaseModel):
    db_source: str
    hubs_to_create: List[HubToCreate] = []
    links_to_create: List[LinkToCreate] = []

    hubs_to_delete: List[str] = []
    sats_to_delete: List[str] = []
    links_to_delete: List[str] = []

    @property
    def hub_names_to_hub_pks(self) -> Dict[str, str]:
        return {hub.name: hub.pk for hub in self.hubs_to_create if hub.pk}
