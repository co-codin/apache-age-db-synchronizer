from typing import Tuple, Iterable

from fastapi import status


class APIError(Exception):
    status_code: int = status.HTTP_400_BAD_REQUEST


class MoreThanTwoFieldsMatchFKPattern(APIError):
    def __init__(self, matched_fks: Tuple[str, ...], fk_pattern: str):
        self._matched_fks = matched_fks
        self._fk_pattern = fk_pattern

    def __str__(self):
        return f"More than 2 fields: {self._matched_fks} match the following fk pattern: {self._fk_pattern}"


class UnknownDBSource(APIError):
    def __init__(self, db_source: str, supported_db_sources: Iterable[str]):
        self._db_source = db_source
        self._supported_db_sources = supported_db_sources

    def __str__(self):
        return f"Unknown db source was given: {self._db_source}, " \
               f"support only the following ones: {list(self._supported_db_sources)}"
