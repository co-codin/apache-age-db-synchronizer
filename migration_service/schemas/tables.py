from dataclasses import dataclass, field
from typing import Dict


@dataclass(frozen=True)
class Table:
    name: str
    fields: Dict[str, str] = field(default_factory=dict)

    def __hash__(self):
        return hash((self.name, self.fields))
