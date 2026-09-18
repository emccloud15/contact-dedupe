"""Stable client-facing output artifacts for a dedupe run."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

    def review_dataframe(
        self, decisions: list[PairDecision], groups: GroupingResult, original: pd.DataFrame,
        record_ids: list[object] | None = None,
    ) -> pd.DataFrame:
        source_values = original[self.record_id_source] if self.record_id_source in original else original.iloc[:, 0]
        ids = record_ids if record_ids is not None else source_values.tolist()
        source_ids = {
            (value if str(value).startswith("record:") else f"record:{value}"): position
            for position, value in enumerate(ids)
        }
        group_by_member = {
            member: group for group in groups.groups for member in group.member_record_ids
        }
        rows: list[dict[str, Any]] = []
        for item in decisions:
            evidence = item.evidence
            group = group_by_member.get(evidence.left_id) or group_by_member.get(evidence.right_id)
            row: dict[str, Any] = {
                "decision": item.decision.value,
                "group_id": group.group_id if group else "",
                "primary_record_id": group.canonical_record_id if group else evidence.left_id,
                "duplicate_record_id": evidence.right_id,
                "match_score": evidence.match_score,
                "matched_fields": self._json(evidence.matched_fields),
                "conflicts": self._json(evidence.conflicts),
                "reason": item.reason,
                "reason_codes": self._json(item.reason_codes),
                "matched_rule": item.matched_rule or "",
                "profile": item.profile,
                "profile_version": item.profile_version,
                "candidate_blocks": self._json(evidence.candidate_blocks),
            }
            for column, field in evidence.fields.items():
                short = column.rsplit(":", 1)[-1]
                row.setdefault(f"{short}_score", field.score)
            for label, record_id in (("primary", evidence.left_id), ("duplicate", evidence.right_id)):
                position = source_ids.get(str(record_id))
                if position is not None:
                    source_row = original.iloc[position]
                    row.update({f"{label}_{column}": value for column, value in source_row.items()})
            rows.append(row)
        columns = [
            "decision", "group_id", "primary_record_id", "duplicate_record_id",
            "match_score", "name_score", "email_score", "phone_score", "address_score",
            "matched_fields", "conflicts", "reason", "reason_codes", "matched_rule",
            "profile", "profile_version", "candidate_blocks",
        ]
        if not rows:
            return pd.DataFrame(columns=columns)
        result = pd.DataFrame(rows)
        return result.reindex(columns=columns + [column for column in result.columns if column not in columns])

    def groups_dataframe(self, groups: GroupingResult) -> pd.DataFrame:
        return pd.DataFrame([
            {
                "group_id": group.group_id,
                "canonical_record_id": group.canonical_record_id,
                "member_count": len(group.member_record_ids),
                "member_record_ids": self._json(group.member_record_ids),
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
        self.groups_dataframe(groups).to_csv(group_file, index=False)
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
