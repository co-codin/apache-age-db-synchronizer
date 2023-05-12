import itertools
import logging

from typing import List, Dict

from pydantic import BaseModel

from migration_service.schemas.tables import HubToCreate, SatToCreate, LinkToCreate, TableToCreate, TableToAlter

logger = logging.getLogger(__name__)


class MigrationIn(BaseModel):
    name: str
    conn_string: str


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
