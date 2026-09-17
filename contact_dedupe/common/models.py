import math
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .exceptions import ConfigError


class ColumnTypeConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    include_name: bool = False
    weight: list[tuple[str, float]] | float = 0.0
    columns: list[str] = Field(default_factory=list)
    combine: list[str] = Field(default_factory=list)

    @field_validator("columns", "combine")
    @classmethod
    def validate_column_names(cls, values: list[str]) -> list[str]:
        if any(not isinstance(value, str) or not value.strip() for value in values):
            raise ValueError("configured column names must be non-empty strings")
        if len(values) != len(set(values)):
            raise ValueError("configured column names must not be duplicated")
        return values

    @field_validator("weight")
    @classmethod
    def validate_weights(cls, value):
        pairs = value if isinstance(value, list) else [(None, value)]
        for field_name, weight in pairs:
            if not isinstance(weight, (int, float)) or not math.isfinite(weight) or not 0 <= weight <= 1:
                label = f" for {field_name!r}" if field_name else ""
                raise ValueError(f"weight{label} must be a finite number between 0 and 1")
        return value

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

    @field_validator("type", "column")
    @classmethod
    def validate_non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("blocking type and column must be non-empty")
        return value


class CandidateBlock(BaseModel):
    type: str
    field: Optional[str] = None
    fields: list[str] = Field(default_factory=list)
    length: Optional[int] = None
    direction: str = "start"
    max_bucket_size: int = 1000

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        value = value.lower().strip()
        if value not in {"exact", "prefix", "composite"}:
            raise ValueError("candidate block type must be exact, prefix, or composite")
        return value

    @field_validator("field")
    @classmethod
    def validate_field(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not value.strip():
            raise ValueError("candidate block field must be a non-empty string")
        return value

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, values: list[str]) -> list[str]:
        if any(not value.strip() for value in values) or len(values) != len(set(values)):
            raise ValueError("candidate block fields must be non-empty and unique")
        return values

    @field_validator("length")
    @classmethod
    def validate_length(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value < 1:
            raise ValueError("candidate prefix length must be positive")
        return value

    @field_validator("direction")
    @classmethod
    def validate_direction(cls, value: str) -> str:
        value = value.lower().strip()
        if value not in {"start", "end"}:
            raise ValueError("candidate prefix direction must be start or end")
        return value

    @field_validator("max_bucket_size")
    @classmethod
    def validate_bucket_size(cls, value: int) -> int:
        if value < 2:
            raise ValueError("candidate max_bucket_size must be at least 2")
        return value

    @model_validator(mode="after")
    def validate_shape(self):
        if self.type in {"exact", "prefix"} and not self.field:
            raise ValueError(f"{self.type} candidate blocks require field")
        if self.type == "composite" and len(self.fields) < 2:
            raise ValueError("composite candidate blocks require at least two fields")
        if self.type == "prefix" and self.length is None:
            raise ValueError("prefix candidate blocks require length")
        return self


class MatchingProfile(BaseModel):
    auto_merge_rules: list[str] = Field(default_factory=list)
    review_rules: list[str] = Field(default_factory=list)

    @field_validator("auto_merge_rules", "review_rules")
    @classmethod
    def validate_rule_names(cls, values: list[str]) -> list[str]:
        supported = {
            "email_exact",
            "phone_exact",
            "email_exact_and_phone_exact",
            "phone_exact_and_name_high",
            "name_high_and_address_high",
        }
        unsupported = sorted(set(values) - supported)
        if unsupported:
            raise ValueError(f"unsupported matching rule(s): {', '.join(unsupported)}")
        return values


class Exclusion(BaseModel):
    column: str

class Bounds(BaseModel):
    u_bound: float = 90.0
    l_bound: float = 75.0

    @field_validator("u_bound", "l_bound")
    @classmethod
    def validate_bound(cls, value: float) -> float:
        if not math.isfinite(value) or not 0 <= value <= 100:
            raise ValueError("matching bounds must be between 0 and 100")
        return value

    @model_validator(mode="after")
    def validate_bound_order(self):
        if self.l_bound > self.u_bound:
            raise ValueError("BOUNDS.l_bound must be less than or equal to BOUNDS.u_bound")
        return self


class ClientConfig(BaseModel):
    CLIENT_NAME: str
    BASE: Optional[bool] = False
    COLUMNS: Columns
    BLOCKING: Blocking
    CANDIDATE_BLOCKS: Optional[list[CandidateBlock]] = None
    MATCHING_PROFILE: str = "legacy_v1"
    MATCHING_PROFILES: dict[str, MatchingProfile] = Field(default_factory=dict)
    EXCLUSION: Optional[Exclusion] = None
    MAIN_MATCH_CRITERIA: str
    MATCH_FIELD: str
    NICKNAME: Optional[str] = None
    BOUNDS: Bounds
    ADDRESS: Optional[bool] = False
    STRICT_MATCH: Optional[bool] = False

    @field_validator("CLIENT_NAME", "MAIN_MATCH_CRITERIA", "MATCH_FIELD")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("required configuration values must be non-empty strings")
        return value

    @field_validator("MATCHING_PROFILE")
    @classmethod
    def validate_profile_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("MATCHING_PROFILE must be a non-empty string")
        return value

    @model_validator(mode="after")
    def validate_selected_profile(self):
        if self.MATCHING_PROFILES and self.MATCHING_PROFILE not in self.MATCHING_PROFILES:
            raise ConfigError(
                f"MATCHING_PROFILE {self.MATCHING_PROFILE!r} is not defined in MATCHING_PROFILES"
            )
        return self

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
    def validate_has_configured_fields(self):
        if not any(field_value is not None for _, field_value in self.COLUMNS):
            raise ConfigError("At least one contact field must be configured in COLUMNS.")
        return self

    @model_validator(mode="after")
    def validate_blocking(self) -> ClientConfig:
        allowed_type = ["zipcode", "state", "id", "name", "idx","contact_type"]
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
