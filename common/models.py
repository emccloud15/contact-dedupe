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

class Blocking(BaseModel):
    type: str
    column: str
    portion: Optional[str] = None

class Bounds(BaseModel):
    u_bound: float
    l_bound: float


class ClientConfig(BaseModel):
    CLIENT_NAME: str
    FILE_PATH: Path
    COLUMNS:  Columns
    GROUP_BY: bool
    BLOCKING: Blocking
    MAIN_MATCH_CRITERIA: str
    MATCH_FIELD: str
    NICKNAME: Optional[str] = None
    BOUNDS: Optional[Bounds] = None
    
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
        if self.MAIN_MATCH_CRITERIA not in allowed:
            raise ConfigError(f"The MAIN_MATCH_CRITERIA value must be one of {allowed}.")
        return self
    
    @model_validator(mode='after')
    def validate_blocking(self):
        allowed_type = ['zipcode','state','id','name']
        allowed_portion = ['start', 'end']
        allowed_cols = [col for _,field_value in self.COLUMNS if field_value is not None for col in field_value.columns]
    
        if self.BLOCKING.type.lower() not in allowed_type:
            raise ConfigError(f"BLOCKING type {self.BLOCKING.type} must be one of {allowed_type}")
        elif self.BLOCKING.column not in allowed_cols:
            raise ConfigError(f"BLOCKING column {self.BLOCKING.column} must be one of {allowed_cols}")
        elif self.BLOCKING.portion.lower() is not None and self.BLOCKING.portion not in allowed_portion:
            raise ConfigError(f"BLOCKING portion {self.BLOCKING.portion} must be one of {allowed_portion}")
        else:
            return self

    
class SystemConfig(BaseModel):
    CLIENT_YAML: Path