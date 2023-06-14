import json
import logging

from enum import Enum

from migration_service.crud.migration import add_migration, select_migration

from migration_service.schemas.migrations import MigrationIn, MigrationPattern

from migration_service.database import db_session
from migration_service.database import ag_session

from migration_service.mq import PikaChannel
from migration_service.services.migration import apply_migration
from migration_service.settings import settings


logger = logging.getLogger(__name__)


class MigrationRequestStatus(Enum):
    SUCCESS = 'success'
    FAILURE = 'failure'


async def synchronize(migration_request: str, channel: PikaChannel):
    migration_request = json.loads(migration_request)

    migration_in = MigrationIn(
        **{
            'name': migration_request['name'],
            'conn_string': migration_request['conn_string'],
            'object_name': migration_request['object_name']
        }
    )
    migration_pattern = MigrationPattern(**migration_request['migration_pattern'])
    source_registry_guid = migration_request['source_registry_guid']
    model = migration_request['model']

    async with db_session() as session:
        with ag_session() as age_session:
            guid = await add_migration(migration_in, session, age_session)
            await apply_migration(migration_pattern, session, age_session)
            graph_migration = await select_migration(session, guid)

            logger.info('Migration request was processed')
            logger.info('Sending result...')
            await channel.basic_publish(
                exchange=settings.migration_exchange,
                routing_key='result',
                body=json.dumps(
                    {
                        'status': MigrationRequestStatus.SUCCESS.value,
                        'conn_string': migration_in.conn_string,
                        'graph_migration': graph_migration.dict(),
                        'source_registry_guid': source_registry_guid,
                        'object_name': migration_in.object_name,
                        'model': model
                    }
                )
            )


async def set_synchronizing_off(migration_request: str, channel: PikaChannel):
    migration_request = json.loads(migration_request)
    source_registry_guid = migration_request['source_registry_guid']

    await channel.basic_publish(
        exchange=settings.migration_exchange,
        routing_key='result',
        body=json.dumps(
            {
                'status': MigrationRequestStatus.FAILURE.value,
                'source_registry_guid': source_registry_guid
            }
        )
    )
