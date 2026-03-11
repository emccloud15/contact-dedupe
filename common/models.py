from pydantic import BaseModel
from pathlib import Path
from typing import Optional,Dict

class ColumnTypeConfig(BaseModel):
    include_name: bool
    include_col: bool


class Columns(BaseModel):
    phone: Optional[dict[str,ColumnTypeConfig]] = None
    email: Optional[dict[str,ColumnTypeConfig]] = None
    address: Optional[dict[str,ColumnTypeConfig]] = None
    name: Optional[dict[str,bool]]= None



class ClientConfig(BaseModel):
    CLIENT_NAME: str
    FILE_PATH: Path
    COLUMNS: Columns

class SystemConfig(BaseModel):
    CLIENT_YAML: Path