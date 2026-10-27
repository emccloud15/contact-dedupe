"""Evaluation metrics for labeled duplicate-pair fixtures."""

from __future__ import annotations

from collections import Counter
from typing import Iterable

from .decision import Decision, PairDecision


def evaluate_decisions(
    decisions: Iterable[PairDecision], duplicate_pairs: set[frozenset[object]],
) -> dict[str, float | int]:
    items = list(decisions)
    generated = {frozenset((x.evidence.left_id, x.evidence.right_id)) for x in items}
    predicted = {
        frozenset((x.evidence.left_id, x.evidence.right_id))
        for x in items if x.decision in {Decision.MERGE, Decision.REVIEW}
    }
    auto = {
        frozenset((x.evidence.left_id, x.evidence.right_id))
        for x in items if x.decision is Decision.MERGE
    }
    recall = len(generated & duplicate_pairs) / len(duplicate_pairs) if duplicate_pairs else 1.0
    precision = len(predicted & duplicate_pairs) / len(predicted) if predicted else 1.0
    auto_precision = len(auto & duplicate_pairs) / len(auto) if auto else 1.0
    return {
        "candidate_pair_count": len(items),
        "candidate_recall": recall,
        "pair_precision": precision,
        "pair_recall": len(predicted & duplicate_pairs) / len(duplicate_pairs) if duplicate_pairs else 1.0,
        "automatic_merge_precision": auto_precision,
        "review_rate": sum(x.decision is Decision.REVIEW for x in items) / len(items) if items else 0.0,
    }
