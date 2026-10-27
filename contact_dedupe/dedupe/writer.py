"""Stable client-facing output artifacts for a dedupe run."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .decision import PairDecision


class ResultWriter:
    def __init__(self, profile: str, profile_version: str | None = None, record_id_source: str | None = None):
        self.profile = profile
        self.profile_version = profile_version or profile
        self.record_id_source = record_id_source

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
    def _display_field_name(field_name: str) -> str:
        source_name = field_name.removeprefix("clean_").split(":", 1)[0]
        return " ".join(part.capitalize() for part in source_name.split("_") if part)

    @classmethod
    def _match_details(cls, item: PairDecision, category: str) -> str:
        details = []
        for field in item.contact_data.get(category, ()):
            left_name = cls._display_field_name(field.left_field)
            right_name = cls._display_field_name(field.right_field)
            match_type = field.match_type.value
            detail = f"{left_name} -> {right_name}: {match_type}"
            if match_type == "FUZZY":
                detail += f" ({field.score:.1f})"
            details.append(detail)
        return "; ".join(details)

    def review_dataframe(
        self, decisions: list[PairDecision], original: pd.DataFrame,
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
        rows: list[dict[str, Any]] = []
        for item in decisions:
            row: dict[str, Any] = {
                "decision": item.decision.value,
                "left_id": record_values.get(str(item.left_id), item.left_id),
                "right_id": record_values.get(str(item.right_id), item.right_id),
                "overall_score": item.overall_score,
                "name_score": item.name_score,
                "address_score": item.address_score,
                "email_score": item.email_score,
                "phone_score": item.phone_score,
                "name_match_details": self._match_details(item, "name"),
                "address_match_details": self._match_details(item, "address"),
                "email_match_details": self._match_details(item, "email"),
                "phone_match_details": self._match_details(item, "phone"),
                "reason": item.reason,
                "matched_rule": item.matched_rule or "",
            }
            for label, record_id in (("left", item.left_id), ("right", item.right_id)):
                position = source_ids.get(str(record_id))
                if position is not None:
                    source_row = original.iloc[position]
                    row.update({f"{label}_{column}": value for column, value in source_row.items()})
            rows.append(row)
        columns = [
            "left_id", "right_id", "decision", "overall_score",
            "name_score", "address_score", "email_score", "phone_score",
        ]
        detail_columns = [
            "name_match_details",
            "address_match_details",
            "email_match_details",
            "phone_match_details",
        ]
        metadata_columns = [
            "reason", "matched_rule",
        ]
        if not rows:
            return pd.DataFrame(columns=columns + detail_columns + metadata_columns)
        result = pd.DataFrame(rows)
        contact_columns = [
            column for column in result.columns
            if column not in columns
            and (column.startswith("left_") or column.startswith("right_"))
        ]
        return result.reindex(
            columns=columns
            + contact_columns
            + detail_columns
            + metadata_columns
            + [
                column for column in result.columns
                if column not in columns + contact_columns + detail_columns + metadata_columns
            ]
        )

    def write(
        self,
        output_dir: str | Path,
        decisions: list[PairDecision],
        original: pd.DataFrame,
        record_ids: list[object] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Path]:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        review = directory / "dedupe_review.csv"
        summary = directory / "run_summary.json"
        self.review_dataframe(decisions, original, record_ids).to_csv(review, index=False)
        counts = pd.Series([item.decision.value for item in decisions]).value_counts().to_dict()
        record_values = self._record_value_map(original, record_ids)
        pair_details = [
            {
                "left_id": record_values.get(str(item.left_id), item.left_id),
                "right_id": record_values.get(str(item.right_id), item.right_id),
                "decision": item.decision.value,
                "candidate_blocks": list(item.evidence.candidate_blocks),
                "contact_data": {
                    category: [
                        {
                            "left_field": field.left_field,
                            "right_field": field.right_field,
                            "left_value": field.left_value,
                            "right_value": field.right_value,
                            "score": field.score,
                            "match_type": field.match_type.value,
                        }
                        for field in comparisons
                    ]
                    for category, comparisons in item.contact_data.items()
                },
            }
            for item in decisions
        ]
        payload = {
            "input_record_count": len(original),
            "candidate_pair_count": len(decisions),
            "decision_counts": counts,
            "pair_details": pair_details,
            "profile_used": self.profile,
            "profile_version": self.profile_version,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            **(metadata or {}),
        }
        summary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        return {"review": review, "summary": summary}
