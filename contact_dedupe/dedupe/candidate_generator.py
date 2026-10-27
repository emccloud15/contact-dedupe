"""Candidate-pair generation, kept separate from duplicate decisions."""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations
import re
from typing import Any, Iterator

import click
import pandas as pd

from contact_dedupe.common.exceptions import ConfigError
from contact_dedupe.common.models import CandidateBlock, ClientConfig
from .normalize import RECORD_ID_COLUMN

DEFAULT_BLOCKS: dict[str, list[CandidateBlock]] = {
    "default_v1" : [
        CandidateBlock(
            type="exact",
            field="email",
            max_bucket_size=500),
        CandidateBlock(
            type="exact",
            field="phone",
            max_bucket_size=500
        ),
        CandidateBlock(
            type="exact",
            field="postal_code",
            length=3,
            direction="start",
            max_bucket_size=1000
        ),
        CandidateBlock(
            type="composite",
            fields=["last_name", "postal_code"],
            max_bucket_size=500
        )
    ]

}
class BlockGenerator:
    """Generate blocking keys for record comparison."""

    @staticmethod
    def _usable(series: pd.Series) -> pd.Series[bool]:
        return series.notna() & (series.astype(str).str.strip() != "")

    @staticmethod
    def _columns_for_fields(df: pd.DataFrame, field: str) -> list[str]:
        if field in df.columns:
            return [field]
        # Permit logical fields (email, phone, address, name) and normalized
        # source-column names to address one or more generated columns.
        matches = [column for column in df.columns if column.endswith(f":{field}")]
        if matches:
            return matches
        normalized = field.lower().replace(" ", "_")
        matches = [
            column for column in df.columns
            if column.lower().replace(" ", "_").startswith(f"clean_{normalized}")
        ]
        if matches:
            return matches

        # Map target logical names such as ``last_name`` or ``postal_code``
        # onto normalized source-column names such as ``clean_Primary Last
        # Name:name`` and ``clean_Primary Address Postal:address``.
        needle = re.sub(r"[^a-z0-9]", "", normalized)
        matches = [
            column for column in df.columns
            if needle and needle in re.sub(r"[^a-z0-9]", "", column.lower())
        ]
        if matches:
            return matches
        raise ConfigError(f"Candidate block field {field!r} is not in the normalized dataframe")

    @staticmethod
    def _prefix(series: pd.Series, length: int, direction: str) -> pd.Series:
        if direction == "start":
            return series.astype(str).str[:length]
        return series.astype(str).str[-length:]

    @staticmethod
    def _composite(df: pd.DataFrame, columns: list[str], mask: pd.Series) -> pd.Series:
        filtered_df = df.loc[mask, columns]
        cleaned_df = filtered_df.astype(str).apply(lambda col: col.str.strip())
        first_col = cleaned_df[columns[0]]
        other_cols = cleaned_df[columns[1:]]
        joined_series = first_col.str.cat(other_cols, sep="|")
        return joined_series.reindex(df.index, fill_value=None)

    @classmethod
    def generate(
        cls, df: pd.DataFrame, block_config: CandidateBlock
    ) -> Iterator[tuple[str, pd.Series]]:
        if block_config.type in {"exact", "prefix"}:
            assert block_config.field
            source_columns = cls._columns_for_fields(df, block_config.field)

            for column in source_columns:
                block_name = f"{block_config.type}:{block_config.field}:{column}"
                values: pd.Series = df[column]

                if block_config.type == "prefix":
                    length = block_config.length
                    if length is None:
                        raise ValueError("prefix candidate block length is required")
                    keys = cls._prefix(
                        series=values,
                        length=length,
                        direction=block_config.direction,
                    )
                    yield block_name, keys.where(cls._usable(values), None)
                else:
                    yield block_name, values.where(cls._usable(values), None)

        elif block_config.type == "composite":
            block_name = f"composite:{'+'.join(block_config.fields)}"
            assert block_config.fields
            source_columns = [
                cls._columns_for_fields(df, field)[0]
                for field in block_config.fields
            ]
            valid_mask = df[source_columns].apply(cls._usable).all(axis=1)
            yield block_name, cls._composite(df=df, columns=source_columns, mask=valid_mask)
        else:
            raise ValueError(f"Unsupported block type {block_config.type}")

class CandidateGenerator:
    """Generate unique candidate pairs and retain their block provenance."""

    def __init__(self, blocks: list[CandidateBlock], record_id_column: str = RECORD_ID_COLUMN):
        self.blocks = blocks
        self.record_id_column = record_id_column

    @classmethod
    def from_config(cls, config: ClientConfig) -> "CandidateGenerator":
        profile_name = config.CANDIDATE_BLOCK_PROFILE
        configured_profiles = config.CANDIDATE_BLOCKS
        blocks = configured_profiles.get(
                profile_name,
                DEFAULT_BLOCKS.get(profile_name, DEFAULT_BLOCKS['default_v1'])
            )
        return cls(blocks)
        



    @staticmethod
    def _usable(value: Any) -> bool:
        return bool(pd.notna(value)) and bool(str(value).strip())

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.record_id_column not in df.columns:
            raise ConfigError(
                f"Normalized dataframe must contain {self.record_id_column!r}"
            )

        pairs: dict[tuple[object, object], set[str]] = defaultdict(set)
        record_ids = df[self.record_id_column].to_numpy()
        with click.progressbar(self.blocks, label="Generating candidate blocks") as block_bar:
            for block in block_bar:
                for block_name, keys in BlockGenerator.generate(df, block):
                    buckets: dict[str, list[int]] = defaultdict(list)

                    for position, key in enumerate(keys.tolist()):
                        if self._usable(key):
                            buckets[str(key)].append(position)

                    for positions in buckets.values():
                        if len(positions) > block.max_bucket_size:
                            continue
                        if len(positions) < 2:
                            continue

                        bucket_ids = record_ids[positions]
                        for left_id, right_id in combinations(bucket_ids, 2):
                            pair = (
                                (left_id, right_id)
                                if str(left_id) < str(right_id)
                                else (right_id, left_id)
                            )
                            pairs[pair].add(block_name)

        sorted_pairs = sorted(pairs.items(), key=lambda item: tuple(map(str, item[0])))
        rows = []
        with click.progressbar(sorted_pairs, label="Generating duplicate candidates") as pairs_bar:
            for (left, right), blocks in pairs_bar:
                rows.append(
                    {
                        "left_record_id": left,
                        "right_record_id": right,
                        "candidate_blocks": tuple(sorted(blocks)),
                    }
                )
        return pd.DataFrame(
            rows,
            columns=["left_record_id", "right_record_id", "candidate_blocks"],
        )
