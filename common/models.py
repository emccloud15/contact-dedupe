from pydantic import BaseModel, model_validator
from pathlib import Path
from typing import Optional


from common.exceptions import ConfigError


class ColumnTypeConfig(BaseModel):
    include_name: bool = False
    weight: Optional[float] = 0.0
    columns: list[str] = []


class NameColumnTypeConfig(BaseModel):
    columns: list[str] = []
    weight: list[tuple[str, float]]


class Columns(BaseModel):
    phone: Optional[ColumnTypeConfig] = None
    email: Optional[ColumnTypeConfig] = None
    address: Optional[ColumnTypeConfig] = None
    name: Optional[NameColumnTypeConfig] = None


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

    @model_validator(mode="after")
    def validate_main_match_criteria(self) -> ClientConfig:
        allowed = []
        for field_name, field_value in self.COLUMNS:
            if field_value is not None:
                if field_name in ["phone", "email"]:
                    allowed.extend(field_value.columns)
                elif field_name == "name":
                    allowed.extend([col for col, _ in field_value.weight])
                else:
                    allowed.append("address")
        if self.MAIN_MATCH_CRITERIA not in allowed:
            raise ConfigError(
                f"The MAIN_MATCH_CRITERIA value must be one of {allowed}."
            )
        return self

    @model_validator(mode="after")
    def validate_blocking(self) -> ClientConfig:
        allowed_type = ["zipcode", "state", "id", "name"]
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
