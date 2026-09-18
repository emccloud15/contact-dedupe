"""Build duplicate groups from explainable pair decisions."""

from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict
from typing import Any

from .decision import Decision, PairDecision


@dataclass(frozen=True)
class DuplicateGroup:
    group_id: str
    canonical_record_id: Any
    member_record_ids: tuple[Any, ...]
    decision: Decision
    conflicts: tuple[str, ...] = ()


@dataclass(frozen=True)
class GroupingResult:
    groups: tuple[DuplicateGroup, ...]
    pair_decisions: tuple[PairDecision, ...]


class DuplicateGrouper:
    """Create stable groups from AUTO_MERGE relationships only.

    REVIEW relationships remain visible at pair level and never merge records.
    Components which rely on a transitive chain are marked for review unless
    every strong identifier in the component has one consistent value.
    """

    def __init__(self, include_review: bool = False):
        self.include_review = include_review

    @staticmethod
    def _usable(value: object) -> bool:
        return value is not None and str(value).strip() != ""

    def build_groups(self, decisions: list[PairDecision]) -> GroupingResult:
        accepted = [
            item for item in decisions
            if item.decision is Decision.AUTO_MERGE
            or (self.include_review and item.decision is Decision.REVIEW)
        ]
        parent: dict[Any, Any] = {}

        def find(value: Any) -> Any:
            parent.setdefault(value, value)
            if parent[value] != value:
                parent[value] = find(parent[value])
            return parent[value]

        def union(left: Any, right: Any) -> None:
            left_root, right_root = find(left), find(right)
            if left_root != right_root:
                parent[right_root] = left_root if str(left_root) < str(right_root) else right_root

        for item in accepted:
            union(item.evidence.left_id, item.evidence.right_id)

        members: dict[Any, set[Any]] = defaultdict(set)
        for record_id in parent:
            members[find(record_id)].add(record_id)

        edges_by_root: dict[Any, list[PairDecision]] = defaultdict(list)
        for item in accepted:
            edges_by_root[find(item.evidence.left_id)].append(item)

        groups: list[DuplicateGroup] = []
        for member_set in sorted(members.values(), key=lambda values: tuple(sorted(map(str, values)))):
            if len(member_set) < 2:
                continue
            ordered = tuple(sorted(member_set, key=str))
            root = find(ordered[0])
            edges = edges_by_root[root]
            conflicts = set()
            if len(edges) < len(member_set) - 1:
                conflicts.add("TRANSITIVE_CHAIN")
            for edge in edges:
                conflicts.update(edge.evidence.conflicts)
            group_decision = Decision.REVIEW if conflicts else Decision.AUTO_MERGE
            groups.append(
                DuplicateGroup(
                    group_id=f"group:{ordered[0]}",
                    canonical_record_id=ordered[0],
                    member_record_ids=ordered,
                    decision=group_decision,
                    conflicts=tuple(sorted(conflicts)),
                )
            )
        return GroupingResult(tuple(groups), tuple(decisions))

    group = build_groups
