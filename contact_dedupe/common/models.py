import math
import re
import warnings
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .exceptions import ConfigError


def rule_name(value: str) -> str:
    """Convert a source column name into its YAML rule-token form."""
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


class ColumnTypeConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    include_name: bool = False
    weight: list[tuple[str, float | None]] | float = 0.0
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
            if weight is None:
                continue
            if not isinstance(weight, (int, float)) or not math.isfinite(weight) or not 0 <= weight <= 1:
                label = f" for {field_name!r}" if field_name else ""
                raise ValueError(f"weight{label} must be a finite number between 0 and 1")
        return value

    @property
    def active_columns(self) -> list[str]:
        """Return the source columns that need a share of this type's weight."""
        return list(dict.fromkeys([*self.columns, *self.combine]))


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
    auto_merge_rule: str | None = None
    auto_ignore_rule: str | None = None

    @field_validator("auto_merge_rule", "auto_ignore_rule")
    @classmethod
    def validate_rule(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("matching rules must be non-empty strings")
        return value.strip() if value is not None else None


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
    CANDIDATE_BLOCK_PROFILE: str = 'default_v1'
    CANDIDATE_BLOCKS: dict[str, list[CandidateBlock]] = Field(default_factory=dict)
    MATCHING_PROFILE: str = "default_v1"
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
    def warn_rule_name_collisions(self):
        group_names = {contact_type for contact_type, config in self.COLUMNS if config is not None}
        for contact_type, config in self.COLUMNS:
            if config is None:
                continue
            if contact_type == "address" and config.columns and not config.combine:
                warnings.warn(
                    "Address columns are configured without 'combine'. "
                    "The address_exact rule requires an address combine block, "
                    "so add 'combine' if address_exact is needed.",
                    UserWarning,
                    stacklevel=2,
                )
            for column in config.active_columns:
                token = rule_name(column)
                if token in group_names:
                    warnings.warn(
                        f"Column {column!r} normalizes to rule name {token!r}, "
                        f"which conflicts with the contact group {token!r}. "
                        "Rename the column and update the YAML to avoid ambiguous rules.",
                        UserWarning,
                        stacklevel=2,
                    )
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

    def _configured_weight_parts(self) -> tuple[dict[str, float], list[str]]:
        """Return explicit weights and columns that still need a weight."""
        explicit: dict[str, float] = {}
        missing: list[str] = []

        for contact_type, config in self.COLUMNS:
            if config is None:
                continue
            columns = config.active_columns
            if isinstance(config.weight, list):
                configured = set(columns)
                configured_weights = {column: weight for column, weight in config.weight}
                for column, weight in config.weight:
                    if column not in configured:
                        raise ConfigError(
                            f"Weight column {column!r} is not configured for contact type {contact_type!r}."
                        )
                    if weight is None or weight <= 0:
                        continue
                    explicit[column] = float(weight)
                missing.extend(
                    f"{contact_type}.{column}"
                    for column in columns
                    if configured_weights.get(column) is None or configured_weights.get(column, 0) <= 0
                )
            elif config.weight > 0 and columns:
                share = float(config.weight) / len(columns)
                explicit.update({column: share for column in columns})
            else:
                missing.extend(f"{contact_type}.{column}" for column in columns)

        return explicit, missing

    def has_unassigned_weights(self) -> bool:
        """Whether any configured source column has no positive weight assignment."""
        _, missing = self._configured_weight_parts()
        return bool(missing)

    def needs_weight_balance(self) -> bool:
        """Whether weights are missing or do not currently total one."""
        explicit, missing = self._configured_weight_parts()
        return bool(missing) or not math.isclose(sum(explicit.values()), 1.0, abs_tol=1e-9)

    def validate_weight_configuration(self) -> ClientConfig:
        explicit, missing = self._configured_weight_parts()
        total = sum(explicit.values())
        if missing:
            warnings.warn(
                "Some configured contact columns have no weight: "
                + ", ".join(missing)
                + ". Choose auto-balance to distribute the remaining weight, or exit.",
                UserWarning,
                stacklevel=2,
            )
        if not math.isclose(total, 1.0, abs_tol=1e-9):
            warnings.warn(
                f"Total contact-column weights are {total:g}, not 1.0. "
                "Choose auto-balance to proportionally normalize them, or exit.",
                UserWarning,
                stacklevel=2,
            )
        return self

    @model_validator(mode="after")
    def validate_contact_column_weights(self) -> ClientConfig:
        return self.validate_weight_configuration()

    def auto_balance_weights(self) -> ClientConfig:
        """Assign missing columns and proportionally normalize all weights to one."""
        explicit, missing = self._configured_weight_parts()
        provisional = dict(explicit)
        default_weight = (
            sum(explicit.values()) / len(explicit) if explicit else 1.0
        )
        for item in missing:
            _, column = item.split(".", 1)
            provisional[column] = default_weight

        total = sum(provisional.values())
        if total <= 0:
            raise ConfigError("Cannot auto-balance contact-column weights because no positive weights were provided.")
        normalized = {column: weight / total for column, weight in provisional.items()}

        for contact_type, config in self.COLUMNS:
            if config is None:
                continue
            columns = config.active_columns
            assigned = {
                column: normalized[column]
                for column in columns
                if column in normalized
            }
            config.weight = [(column, assigned[column]) for column in columns]
        return self

    def weight_for(self, contact_type: str, column: str) -> float:
        """Return the resolved weight for one normalized source column."""
        config = getattr(self.COLUMNS, contact_type, None)
        if config is None:
            return 1.0
        if isinstance(config.weight, list):
            return float(dict(config.weight).get(column) or 0.0)
        columns = config.active_columns
        return float(config.weight) / len(columns) if columns else 0.0
