from pydantic import BaseModel, model_validator
from pathlib import Path
from typing import Optional

from common.exceptions import ConfigError


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
    MAIN_MATCH_CRITERIA: str
    NICKNAME: Optional[str] = None
    BOUNDS: Optional[list[dict[str,float]]] = None
    
    @model_validator(mode='after')
    def validate_main_match_criteria(self):
        allowed = []
        for field_name,field_value in self.COLUMNS:
            if field_value is not None:
                if field_name in ['phone', 'email']:
                    allowed.extend(field_value.columns)
                elif field_name == 'name':
                    allowed.extend([col for col,_ in field_value.weight])
                else:
                    allowed.append('address')
        print(allowed)
        if self.MAIN_MATCH_CRITERIA not in allowed:
            raise ConfigError(f"The MAIN_MATCH_CRITERIA value must be one of {allowed}.")
        return self


class SystemConfig(BaseModel):
    CLIENT_YAML: Path