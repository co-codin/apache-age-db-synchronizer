from dataclasses import dataclass, field
from typing import Dict


@dataclass(frozen=True)
class Table:
    name: str
    field_to_type: Dict[str, str] = field(default_factory=dict)

    def __hash__(self):
        return hash((self.name, self.field_to_type))
