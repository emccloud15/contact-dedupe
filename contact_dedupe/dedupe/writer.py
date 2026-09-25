"""Stable client-facing output artifacts for a dedupe run."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import re

import pandas as pd

from .decision import PairDecision
from .grouping import GroupingResult


class ResultWriter:
    def __init__(self, profile: str, profile_version: str | None = None, record_id_source: str | None = None):
        self.profile = profile
        self.profile_version = profile_version or profile
        self.record_id_source = record_id_source

    @staticmethod
    def _json(value: Any) -> str:
        if isinstance(value, (list, tuple, set)):
            return json.dumps(list(value), sort_keys=True)
        return "" if value is None else str(value)

    @staticmethod
    def _column_token(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")

    def _record_value_map(
        self,
        original: pd.DataFrame,
        record_ids: list[object] | None,
    ) -> dict[str, Any]:
        source_values = (
            original[self.record_id_source]
            if self.record_id_source in original
            else original.iloc[:, 0]
        )
        ids = record_ids if record_ids is not None else source_values.tolist()
        result: dict[str, Any] = {}
        for record_id, value in zip(ids, source_values.tolist()):
            token = str(record_id)
            result[token] = value
            if not token.startswith("record:"):
                result[f"record:{token}"] = value
        return result

    @staticmethod
    def _average_group_scores(evidence) -> dict[str, float | None]:
        scores: dict[str, list[float]] = {}
        for field in evidence.fields.values():
            if field.score is not None:
                scores.setdefault(field.contact_type, []).append(field.score)
        return {
            contact_type: sum(values) / len(values)
            for contact_type, values in scores.items()
        }

    def review_dataframe(
        self, decisions: list[PairDecision], groups: GroupingResult, original: pd.DataFrame,
        record_ids: list[object] | None = None,
    ) -> pd.DataFrame:
        source_values = (
            original[self.record_id_source]
            if self.record_id_source in original
            else original.iloc[:, 0]
        )
        ids = record_ids if record_ids is not None else source_values.tolist()
        source_ids: dict[str, int] = {}
        for position, value in enumerate(ids):
            token = str(value)
            source_ids[token] = position
            if not token.startswith("record:"):
                source_ids[f"record:{token}"] = position
        record_values = self._record_value_map(original, record_ids)
        group_by_member = {
            member: group for group in groups.groups for member in group.member_record_ids
        }
        rows: list[dict[str, Any]] = []
        for item in decisions:
            evidence = item.evidence
            group = group_by_member.get(evidence.left_id) or group_by_member.get(evidence.right_id)
            group_id = record_values.get(str(group.canonical_record_id), "") if group else ""
            row: dict[str, Any] = {
                "decision": item.decision.value,
                "group_id": group_id,
                "primary_id": record_values.get(str(evidence.left_id), evidence.left_id),
                "duplicate_id": record_values.get(str(evidence.right_id), evidence.right_id),
                "match_score": evidence.match_score,
                "candidate_blocks": self._json(evidence.candidate_blocks),
            }
            row.update({
                f"{contact_type}_score": score
                for contact_type, score in self._average_group_scores(evidence).items()
            })
            for label, record_id in (("primary", evidence.left_id), ("duplicate", evidence.right_id)):
                position = source_ids.get(str(record_id))
                if position is not None:
                    source_row = original.iloc[position]
                    row.update({f"{label}_{column}": value for column, value in source_row.items()})
            for field in evidence.fields.values():
                token = self._column_token(field.source_name)
                row[f"field_{token}_score"] = field.score
                row[f"field_{token}_match_type"] = (
                    field.match_type.value
                    if hasattr(field.match_type, "value")
                    else field.match_type
                )
            rows.append(row)
        columns = [
            "decision", "group_id", "primary_id", "duplicate_id",
            "match_score", "name_score", "email_score", "phone_score", "address_score",
            "reason", "matched_rule", "candidate_blocks",
        ]
        if not rows:
            return pd.DataFrame(columns=columns)
        result = pd.DataFrame(rows)
        contact_columns = [
            column for column in result.columns
            if column not in columns
            and (column.startswith("primary_") or column.startswith("duplicate_"))
        ]
        evidence_columns = [
            column for column in result.columns
            if column.startswith("field_")
        ]
        return result.reindex(
            columns=columns
            + contact_columns
            + evidence_columns
            + [
                column for column in result.columns
                if column not in columns + contact_columns + evidence_columns
            ]
        )

    def groups_dataframe(
        self,
        groups: GroupingResult,
        original: pd.DataFrame | None = None,
        record_ids: list[object] | None = None,
    ) -> pd.DataFrame:
        record_values = (
            self._record_value_map(original, record_ids)
            if original is not None
            else {}
        )
        return pd.DataFrame([
            {
                "group_id": record_values.get(str(group.canonical_record_id), group.group_id),
                "canonical_record_id": record_values.get(
                    str(group.canonical_record_id), group.canonical_record_id
                ),
                "member_count": len(group.member_record_ids),
                "member_record_ids": self._json([
                    record_values.get(str(member), member)
                    for member in group.member_record_ids
                ]),
                "decision": group.decision.value,
                "group_conflicts": self._json(group.conflicts),
            }
            for group in groups.groups
        ])

    def write(
        self,
        output_dir: str | Path,
        decisions: list[PairDecision],
        groups: GroupingResult,
        original: pd.DataFrame,
        record_ids: list[object] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Path]:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        review = directory / "dedupe_review.csv"
        group_file = directory / "duplicate_groups.csv"
        summary = directory / "run_summary.json"
        self.review_dataframe(decisions, groups, original, record_ids).to_csv(review, index=False)
        self.groups_dataframe(groups, original, record_ids).to_csv(group_file, index=False)
        counts = pd.Series([item.decision.value for item in decisions]).value_counts().to_dict()
        payload = {
            "input_record_count": len(original),
            "candidate_pair_count": len(decisions),
            "decision_counts": counts,
            "duplicate_group_count": len(groups.groups),
            "profile_used": self.profile,
            "profile_version": self.profile_version,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            **(metadata or {}),
        }
        summary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return {"review": review, "groups": group_file, "summary": summary}
