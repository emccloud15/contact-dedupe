"""Explainable field-level comparison for generated candidate pairs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

import pandas as pd
from nicknames import NickNamer
from rapidfuzz.fuzz import WRatio

from contact_dedupe.common.models import ClientConfig
from .normalize import RECORD_ID_COLUMN


@dataclass(frozen=True)
class FieldEvidence:
    field: str
    score: float | None
    exact: bool
    available: bool


@dataclass(frozen=True)
class MatchEvidence:
    left_id: Any
    right_id: Any
    fields: dict[str, FieldEvidence]
    matched_fields: list[str]
    missing_fields: list[str]
    used_fields: list[str]
    conflicts: list[str]
    candidate_blocks: tuple[str, ...] = field(default_factory=tuple)
    match_score: float | None = None


class EvidenceBuilder:
    """Compare all normalized fields for each candidate pair."""

    def __init__(self, client_cfg: ClientConfig | None = None):
        self.client_cfg = client_cfg
        self.nickname_finder = NickNamer()

    @staticmethod
    def _available(value: object) -> bool:
        return cast(bool, pd.notna(value)) and bool(str(value).strip())

    @staticmethod
    def _field_type(column: str) -> str:
        return column.rsplit(":", 1)[-1]

    @staticmethod
    def _source_name(column: str) -> str:
        return column.removeprefix("clean_").rsplit(":", 1)[0]

    def _is_nickname_match(self, left: object, right: object) -> bool:
        left_name = str(left).lower()
        right_name = str(right).lower()
        return bool(
            {left_name, right_name}
            & set(self.nickname_finder.nicknames_of(left_name))
            and right_name in set(self.nickname_finder.nicknames_of(left_name))
        )

    def _field_weight(self, column: str) -> float:
        if self.client_cfg is None:
            return 1.0
        field_type = self._field_type(column)
        config = getattr(self.client_cfg.COLUMNS, field_type, None)
        if config is None:
            return 1.0
        source = self._source_name(column)
        if isinstance(config.weight, list):
            for configured_name, weight in config.weight:
                if configured_name == source:
                    return float(weight)
            return 0.0
        return float(config.weight)

    def build_pair(
        self,
        normalized: pd.DataFrame,
        left_id: object,
        right_id: object,
        candidate_blocks: tuple[str, ...] = (),
    ) -> MatchEvidence:
        rows = normalized.set_index(RECORD_ID_COLUMN, drop=False)
        left = rows.loc[left_id]
        right = rows.loc[right_id]
        fields: dict[str, FieldEvidence] = {}
        matched: list[str] = []
        missing: list[str] = []
        used: list[str] = []
        conflicts: list[str] = []
        weighted_score = 0.0
        active_weight = 0.0

        comparison_columns = [
            column for column in normalized.columns
            if column.startswith("clean_") and ":" in column
        ]
        for column in comparison_columns:
            left_value, right_value = left[column], right[column]
            available = self._available(left_value) and self._available(right_value)
            if not available:
                fields[column] = FieldEvidence(column, None, False, False)
                missing.append(column)
                continue

            exact = str(left_value) == str(right_value)
            score = 100.0 if exact else float(WRatio(str(left_value), str(right_value)))
            if self._field_type(column) == "name" and self._is_nickname_match(left_value, right_value):
                score = 100.0
                exact = True
            fields[column] = FieldEvidence(column, score, exact, True)
            used.append(column)
            if exact:
                matched.append(column)
            if self._field_type(column) in {"email", "phone"} and not exact:
                conflicts.append(column)
            weight = self._field_weight(column)
            weighted_score += score * weight
            active_weight += weight

        return MatchEvidence(
            left_id=left_id,
            right_id=right_id,
            fields=fields,
            matched_fields=matched,
            missing_fields=missing,
            used_fields=used,
            conflicts=conflicts,
            candidate_blocks=tuple(candidate_blocks),
            match_score=(weighted_score / active_weight) if active_weight else None,
        )

    def build_all(
        self,
        normalized: pd.DataFrame,
        candidates: pd.DataFrame,
    ) -> list[MatchEvidence]:
        return [
            self.build_pair(
                normalized,
                row["left_record_id"],
                row["right_record_id"],
                tuple(row["candidate_blocks"]),
            )
            for row in candidates.to_dict(orient="records")
        ]
