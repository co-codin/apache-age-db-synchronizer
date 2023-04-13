import re
from typing import Tuple

from fastapi import status


class APIError(Exception):
    status_code: int = status.HTTP_400_BAD_REQUEST


class NoNeo4jConnection(APIError):
    def __init__(self, neo4j_conn_string):
        self.status_code = status.HTTP_400_BAD_REQUEST
        self._neo4j_conn_string = neo4j_conn_string

    def __str__(self):
        return f"No connection with {self._neo4j_conn_string}"


class MoreThanTwoFieldsMatchFKPattern(Exception):
    def __init__(self, matched_fks: Tuple[str, ...], fk_pattern: str):
        self._matched_fks = matched_fks
        self._fk_pattern = fk_pattern

    def __str__(self):
        return f"More than 2 fields: {self._matched_fks} match the following fk pattern: {self._fk_pattern}"
