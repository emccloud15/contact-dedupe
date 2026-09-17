"""Candidate-pair generation, kept separate from duplicate decisions."""

from __future__ import annotations

from collections import defaultdict
import re
from typing import Iterator, cast

import pandas as pd

from contact_dedupe.common.exceptions import ConfigError
from contact_dedupe.common.models import CandidateBlock, ClientConfig
from .normalize import RECORD_ID_COLUMN


class CandidateGenerator:
    """Generate unique candidate pairs and retain their block provenance."""

    def __init__(self, blocks: list[CandidateBlock], record_id_column: str = RECORD_ID_COLUMN):
        self.blocks = blocks
        self.record_id_column = record_id_column

    @classmethod
    def from_config(cls, config: ClientConfig) -> "CandidateGenerator":
        if config.CANDIDATE_BLOCKS:
            return cls(config.CANDIDATE_BLOCKS)

        # Preserve the existing single-block configuration as legacy_v1.
        blocking = config.BLOCKING
        if blocking.portion:
            block = CandidateBlock(
                type="prefix",
                field=blocking.column,
                length=3,
                direction=blocking.portion,
            )
        else:
            block = CandidateBlock(type="exact", field=blocking.column)
        return cls([block])

    def _columns_for_field(self, df: pd.DataFrame, field: str) -> list[str]:
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
    def _usable(value: object) -> bool:
        return cast(bool, pd.notna(value)) and bool(str(value).strip())

    def _keys_for_block(
        self, df: pd.DataFrame, block: CandidateBlock
    ) -> Iterator[tuple[str, pd.Series]]:
        if block.type in {"exact", "prefix"}:
            assert block.field is not None
            source_columns = self._columns_for_field(df, block.field)
            for column in source_columns:
                values = cast(pd.Series, df[column])
                if block.type == "prefix":
                    length = block.length
                    if length is None:
                        raise ValueError("prefix candidate block length is required")
                    prefix_length: int = length
                    values = values.map(
                        lambda value: (
                            str(value)[:prefix_length]
                            if block.direction == "start"
                            else str(value)[-prefix_length:]
                        ) if self._usable(value) else None
                    )
                yield f"{block.type}:{block.field}:{column}", cast(pd.Series, values)
            return

        source_columns = [
            self._columns_for_field(df, field)[0] for field in block.fields
        ]
        values = cast(pd.Series, df[source_columns].apply(
            lambda row: "|".join(str(value).strip() for value in row)
            if all(self._usable(value) for value in row)
            else None,
            axis=1,
        ))
        yield f"composite:{'+'.join(block.fields)}", values

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.record_id_column not in df.columns:
            raise ConfigError(
                f"Normalized dataframe must contain {self.record_id_column!r}"
            )

        pairs: dict[tuple[object, object], set[str]] = defaultdict(set)
        for block in self.blocks:
            for block_name, keys in self._keys_for_block(df, block):
                buckets: dict[str, list[int]] = defaultdict(list)
                for position, key in enumerate(keys.tolist()):
                    if self._usable(key):
                        buckets[str(key)].append(position)

                for positions in buckets.values():
                    if len(positions) > block.max_bucket_size:
                        continue
                    for left_position, left in enumerate(positions[:-1]):
                        for right in positions[left_position + 1:]:
                            left_id = df.iloc[left][self.record_id_column]
                            right_id = df.iloc[right][self.record_id_column]
                            pair = tuple(sorted((left_id, right_id), key=str))
                            pairs[pair].add(block_name)

        rows = [
            {
                "left_record_id": left,
                "right_record_id": right,
                "candidate_blocks": tuple(sorted(blocks)),
            }
            for (left, right), blocks in sorted(pairs.items(), key=lambda item: tuple(map(str, item[0])))
        ]
        return pd.DataFrame(
            rows,
            columns=["left_record_id", "right_record_id", "candidate_blocks"],
        )
