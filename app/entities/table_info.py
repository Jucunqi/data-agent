from dataclasses import dataclass


@dataclass
class TableInfo:
    id: str
    name: str | None = None
    role: str | None = None
    description: str | None = None
