import re

from datetime import datetime

from sqlalchemy import Column, BigInteger, String, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from migration_service.database import Base


class Migration(Base):
    __tablename__ = "migrations"

    id = Column(BigInteger, primary_key=True, autoincrement=True, nullable=False)
    parent_id = Column(BigInteger, ForeignKey('migrations.id'))

    guid = Column(String(36), nullable=False, index=True, unique=True)
    name = Column(String(110), nullable=False)
    db_source = Column(String(36), nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, server_default=func.now())
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow, server_onupdate=func.now()
    )

    tables = relationship('Table')
    prev_migration = relationship('Migration', remote_side=[id], uselist=False, backref='next_migration')


class Table(Base):
    __tablename__ = "tables"

    id = Column(BigInteger, primary_key=True, autoincrement=True, nullable=False)
    migration_id = Column(BigInteger, ForeignKey(Migration.id))
    old_name = Column(String(110))
    new_name = Column(String(110))

    fields = relationship('Field')

    def fk_count(self, pattern: re.Pattern) -> int:
        count = 0
        for field in self.fields:
            field_name = field.new_name if field.new_name else field.old_name
            if pattern.search(field_name):
                count += 1
        return count


class Field(Base):
    __tablename__ = "fields"

    id = Column(BigInteger, primary_key=True, autoincrement=True, nullable=False)
    table_id = Column(BigInteger, ForeignKey(Table.id))
    old_name = Column(String(110))
    new_name = Column(String(110))
    old_type = Column(String(36))
    new_type = Column(String(36))
