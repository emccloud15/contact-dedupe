"""Explainable field-level comparison for generated candidate pairs."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal, Any, cast

import click

import pandas as pd
from nicknames import NickNamer
from rapidfuzz.fuzz import WRatio

from contact_dedupe.common.models import ClientConfig
from .normalize import RECORD_ID_COLUMN


class MatchType(StrEnum):
    FUZZY = "FUZZY"
    NICKNAME = "NICKNAME"
    EXACT = "EXACT"

@dataclass(frozen=True)
class FieldEvidence:
    field: str
    score: float | None
    exact: bool
    match_type: str | None
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

    def __init__(self, client_cfg: ClientConfig | None):
        self.client_cfg = client_cfg
        self.nickname_finder = NickNamer()
        self._nickname_cache: dict[str, set[str]] = {}

    @staticmethod
    def _available(value: Any) -> bool:
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
        if left_name not in self._nickname_cache:
            self._nickname_cache[left_name] = set(self.nickname_finder.nicknames_of(left_name)) | {left_name}
        if right_name not in self._nickname_cache:
            self._nickname_cache[right_name] = set(self.nickname_finder.nicknames_of(right_name)) | {right_name}
        return bool(
            self._nickname_cache[left_name] & self._nickname_cache[right_name]
        )

    def _field_weight(self, column: str) -> float:
        if self.client_cfg is None:
            return 1.0
        field_type = self._field_type(column)
        source = self._source_name(column)
        return self.client_cfg.weight_for(field_type, source)

    def _build_pair_from_rows(
        self, rows: pd.DataFrame, comparison_columns: list[str], left_id: Any,
        right_id: Any, candidate_blocks: tuple[str, ...] = (),
    ) -> MatchEvidence:
        left = rows.loc[left_id] 
        right = rows.loc[right_id]
        fields: dict[str, FieldEvidence] = {}
        matched: list[str] = []
        missing: list[str] = []
        used: list[str] = []
        conflicts: list[str] = []
        weighted_score = 0.0
        active_weight = 0.0

        for column in comparison_columns:
            left_value, right_value = left[column], right[column]
            available = self._available(left_value) and self._available(right_value)
            if not available:
                fields[column] = FieldEvidence(column, None, False, None, False)
                missing.append(self._source_name(column))
                continue

            exact = str(left_value) == str(right_value)
            score = 100.0 if exact else float(WRatio(str(left_value), str(right_value)))            
            match_type = MatchType.EXACT if exact else MatchType.FUZZY

            assert self.client_cfg
            if self._source_name(column) == self.client_cfg.NICKNAME and self._is_nickname_match(left_value, right_value):
                score = 100.0
                match_type = MatchType.NICKNAME
            fields[column] = FieldEvidence(column, score, exact, match_type, True)
            used.append(self._source_name(column))
            if exact:
                matched.append(self._source_name(column))
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

    def build_pair(
        self,
        normalized: pd.DataFrame,
        left_id: object,
        right_id: object,
        candidate_blocks: tuple[str, ...] = (),
    ) -> MatchEvidence:
        rows = normalized.set_index(RECORD_ID_COLUMN, drop=False)
        comparison_columns = [
            column for column in normalized.columns
            if column.startswith("clean_") and ":" in column
        ]
        return self._build_pair_from_rows(rows, comparison_columns, left_id, right_id, candidate_blocks)

    def build_all(
        self,
        normalized: pd.DataFrame,
        candidates: pd.DataFrame,
    ) -> list[MatchEvidence]:
        rows = normalized.set_index(RECORD_ID_COLUMN, drop=False)
        comparison_columns = [
            column for column in normalized.columns
            if column.startswith("clean_") and ":" in column
        ]
        final = []
        with click.progressbar(candidates.to_dict(orient="records")) as bar:
            for row in bar:
                final.append(
                    self._build_pair_from_rows(
                    rows, comparison_columns,
                    row["left_record_id"],
                    row["right_record_id"],
                    tuple(row["candidate_blocks"]),
                    )
                )
        return final


        
