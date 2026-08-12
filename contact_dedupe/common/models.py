from pydantic import BaseModel, model_validator, field_validator
from typing import Optional


from .exceptions import ConfigError


class ColumnTypeConfig(BaseModel):
    include_name: bool = False
    weight: list[tuple[str, float]] | float = 0.0
    columns: list[str]  = []
    combine: list[str] = []

class Columns(BaseModel):
    phone: Optional[ColumnTypeConfig] = None
    email: Optional[ColumnTypeConfig] = None
    address: Optional[ColumnTypeConfig] = None
    name: Optional[ColumnTypeConfig] = None

class Blocking(BaseModel):
    strict: bool
    type: str
    column: str
    portion: Optional[str] = None


class Bounds(BaseModel):
    u_bound: float = 90.0
    l_bound: float = 75.0


class ClientConfig(BaseModel):
    CLIENT_NAME: str
    COLUMNS: Columns
    BLOCKING: Blocking
    MAIN_MATCH_CRITERIA: str
    MATCH_FIELD: str
    NICKNAME: Optional[str] = None
    BOUNDS: Bounds
    ADDRESS: Optional[bool] = False
    STRICT_MATCH: Optional[bool] = False

    @model_validator(mode="after")
    def validate_main_match_criteria(self) -> ClientConfig:
        allowed = ['address']
        for _,field_value in self.COLUMNS:
            if field_value is not None:
                allowed.extend(field_value.columns)
                
        if self.MAIN_MATCH_CRITERIA not in allowed:
            raise ConfigError(
                f"The MAIN_MATCH_CRITERIA value must be one of {allowed}."
            )
        return self

    @model_validator(mode="after")
    def validate_blocking(self) -> ClientConfig:
        allowed_type = ["zipcode", "state", "id", "name", "idx"]
        allowed_portion = ["start", "end"]

        if self.BLOCKING.type.lower() not in allowed_type:
            raise ConfigError(
                f"BLOCKING type {self.BLOCKING.type} must be one of {allowed_type}"
            )
        elif (
            self.BLOCKING.portion is not None
            and self.BLOCKING.portion.lower() not in allowed_portion
        ):
            raise ConfigError(
                f"BLOCKING portion {self.BLOCKING.portion} must be one of {allowed_portion}"
            )
        else:
            return self

    @model_validator(mode='after')
    def validate_at_least_one_has_data(self):
        for ct in self.COLUMNS:
            if ct[1] is not None:
                if not ct[1].columns and not ct[1].combine:
                    raise ConfigError(f"Both 'columns' and 'combine' can not be empty for field: '{ct[0]}'.")
        return self
    @model_validator(mode='after')
    def validate_combine_has_multiple_fields(self):
        for ct in self.COLUMNS:
            if ct[1] is not None:
                if ct[1].combine:
                    if len(ct[1].combine) < 2:
                        raise ConfigError(f"To use the 'combine' setting for: '{ct[0]}' at least two fields must be listed. One field can not be combined with itself\n Current 'combine' listed fields: {ct[1].combine}")
        return self

