import itertools
import logging

from typing import List, Dict

from pydantic import BaseModel, validator

from migration_service.schemas.tables import HubToCreate, SatToCreate, LinkToCreate, TableToCreate, TableToAlter
from migration_service.errors import UnknownDBSource
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


class MigrationOut(BaseModel):
    name: str
    tables_to_create: List[TableToCreate] = []
    tables_to_alter: List[TableToAlter] = []
    tables_to_delete: List[str] = []


class MigrationPattern(BaseModel):
    hub_prefix: str = r'\w*'
    pk_pattern = "hash_key"

    fk_table = f"^({hub_prefix})_sat$"
    fk_pattern = f"^(?:id)?({hub_prefix})_hash_fkey$"


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
