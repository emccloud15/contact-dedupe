"""Explainable field-level comparison for generated candidate pairs."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Optional, Any, cast, Hashable

from itertools import product
from collections import defaultdict

import click

import pandas as pd
from nicknames import NickNamer
from rapidfuzz.fuzz import WRatio

from contact_dedupe.common.models import ClientConfig, Columns
from .normalize import RECORD_ID_COLUMN


class MatchType(StrEnum):
    FUZZY = "FUZZY"
    NICKNAME = "NICKNAME"
    EXACT = "EXACT"
    NULL_VALUE = "NULL_VALUE"
    NO_MATCH = "NO_MATCH"

@dataclass
class ComparisonColumns:
    type: str
    columns: list[str]

@dataclass
class FieldEvidence:
    left_field: str
    right_field: str
    left_value: Optional[str]
    right_value: Optional[str]
    score: float
    match_type: MatchType

@dataclass
class CategoryEvidence:
    category: str
    aggregate_score: float
    comparisons: list[FieldEvidence] = field(default_factory=list)

@dataclass(frozen=True)
class MatchEvidence:
    left_id: Any
    right_id: Any
    overall_score: float
    categories: dict[str, CategoryEvidence] = field(default_factory=dict)
    candidate_blocks: tuple[str, ...] = ()


class EvidenceBuilder:
    """Compare all normalized fields for each candidate pair."""

    def __init__(self, client_cfg: ClientConfig | None = None):
        self.client_cfg = client_cfg
        self.nickname_finder = NickNamer()
        self._nickname_cache: dict[str, set[str]] = {}
        self._field_scores: dict[str,float] = defaultdict(float)
        self._weighted_scores: dict[str,float] = defaultdict(float)

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
    
    def _get_field_evidence(
            self, source: str, left_value: Any, right_value: Any, 
            left_field: str, right_field: str
        ) -> FieldEvidence:

        
        exact = str(left_value) == str(right_value)

        # Dont fuzzy on phones as one digit different is a completely different phone number
        if source == 'phone':
            score = 100.0 if exact else 0.0
        else:
            score = 100.0 if exact else float(WRatio(left_value, right_value))

        

        if (
            self.client_cfg
            and self.client_cfg.NICKNAME
            and (self._source_name(left_field) or self._source_name(right_field)) == self.client_cfg.NICKNAME
            and score != 100.0
            and self._is_nickname_match(left_value, right_value)
        ):
            score = 100.0


        assert self.client_cfg

        if exact:
            match_type = MatchType.EXACT
        elif score > self.client_cfg.BOUNDS.l_bound:
            match_type = MatchType.FUZZY
        elif score == 100.0 and not exact:
            match_type = MatchType.NICKNAME
        else:
            match_type = MatchType.NO_MATCH
    

        if score < self.client_cfg.BOUNDS.l_bound:
            match_type = MatchType.NO_MATCH

        
        return FieldEvidence(
            left_field=left_field,
            right_field=right_field,
            left_value=left_value,
            right_value=right_value,
            score=score,
            match_type=match_type
        )
        

        
    def _get_category_evidence(
            self, 
            rows: dict[Hashable, dict[Hashable,Any]], 
            left_id: str, right_id: str, 
            category: str, columns: list[str]
        ) -> CategoryEvidence:

        fields: list[FieldEvidence]
        aggregate_score = 0.0
        left = rows[left_id]
        right = rows[right_id]
    
        if category == 'phone' or category == 'email':
            # Create a list for the left and right fields so we can compare the dot product. 
            left_columns = [col for col in columns if pd.notna(left[col])]
            right_columns = [col for col in columns if pd.notna(right[col])]
            
            fields = [
                self._get_field_evidence(
                    source=category,
                    left_value=left[left_col], 
                    right_value=right[right_col],
                    left_field=left_col,
                    right_field=right_col
                )
                for left_col, right_col in product(left_columns, right_columns)
            ]
            
        else:
            
            fields = [
                self._get_field_evidence(
                    source=category,
                    left_value=left[col],
                    right_value=right[col],
                    left_field=col,
                    right_field=col
            )
            for col in columns
        ]
        aggregate_score += sum(field.score for field in fields) / len(fields) if len(fields) else 0.0

        return CategoryEvidence(
            category=category,
            aggregate_score=aggregate_score,
            comparisons=fields
        )
        
    
    def _build_pair_from_rows(
        self, rows: dict[Hashable, dict[Hashable,Any]], comparison_columns: list[ComparisonColumns], left_id: str,
        right_id: str, candidate_blocks: tuple[str, ...] = (),
    ) -> MatchEvidence:
        category_data: dict[str, CategoryEvidence] = {}
        match_score = 0.0
        

        assert self.client_cfg

        for column in comparison_columns:

            category_data.update({column.type: self._get_category_evidence(rows, left_id, right_id, column.type, column.columns)})

        match_score += sum(category_data[column.type].aggregate_score for column in comparison_columns) / 4

        return MatchEvidence(
            left_id=left_id,
            right_id=right_id,
            overall_score=match_score,
            categories=category_data,
            candidate_blocks=candidate_blocks
        )

    def build_all(
        self,
        normalized: pd.DataFrame,
        candidates: pd.DataFrame
    ) -> list[MatchEvidence]:
        rows = normalized.set_index(RECORD_ID_COLUMN, drop=False).to_dict(orient="index")

        
        assert self.client_cfg
        comparison_map: dict[str, list[str]] = {
            ct_type[0]:
            [c for c in normalized.columns if self._field_type(c) == ct_type[0]] 
            for ct_type in self.client_cfg.COLUMNS
        }

        comparison_columns = [
            ComparisonColumns(type=name, columns=cols) 
            for name, cols in comparison_map.items()
        ]

        final = []
        with click.progressbar(candidates.to_dict(orient="records")) as bar:
            for row in bar:
                if row["left_record_id"] == row["right_record_id"]:
                    continue
                final.append(
                    self._build_pair_from_rows(
                    rows, comparison_columns,
                    row["left_record_id"],
                    row["right_record_id"],
                    tuple(row["candidate_blocks"]),
                    )
                )
        return final


        
