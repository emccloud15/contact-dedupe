from pydantic import BaseModel
from pathlib import Path
from typing import Optional,Union


class ColumnTypeConfig(BaseModel):
    include_name: bool = False
    weight: float
    columns: list[str] = []
class NameColumnTypeConfig(BaseModel):
    columns: list[str] = []
    weight: list[tuple[str,float]]

class Columns(BaseModel):
    phone: Optional[ColumnTypeConfig] = None
    email: Optional[ColumnTypeConfig] = None
    address: Optional[ColumnTypeConfig] = None
    name: Optional[NameColumnTypeConfig] = None

class ClientConfig(BaseModel):
    CLIENT_NAME: str
    FILE_PATH: Path
    COLUMNS:  Columns
    BLOCKING: str
    BOUNDS: Optional[list[dict[str,float]]] = None

class SystemConfig(BaseModel):
    CLIENT_YAML: Path