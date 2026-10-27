"""Versioned, explainable pair-level matching decisions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re

from typing import Any

from contact_dedupe.common.exceptions import ConfigError
from contact_dedupe.common.models import ClientConfig, MatchingProfile
from .evidence import MatchEvidence, FieldEvidence, MatchType


class Decision(StrEnum):
    MERGE = "MERGE"
    REVIEW = "REVIEW"
    IGNORE = "IGNORE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@dataclass(frozen=True)
class PairDecision:
    left_id: Any
    right_id: Any
    decision: Decision
    overall_score: float
    name_score: float | None
    email_score: float | None
    phone_score: float | None
    address_score: float | None
    match_types: dict[str, tuple[MatchType, ...]]
    contact_data: dict[str, tuple[FieldEvidence, ...]]
    matched_rule: str | None
    reason_codes: tuple[str, ...]
    reason: str
    profile: str
    profile_version: str
    evidence: MatchEvidence


@dataclass(frozen=True)
class PredicateSpec:
    kind: str
    operator: str
    column: str | None = None
    contact_type: str | None = None

@dataclass
class RuleNode:
    op: str
    children: list[RuleNode] | None = None
    token: str | None = None



DEFAULT_PROFILES: dict[str, MatchingProfile] = {
    "default_v1": MatchingProfile(
        auto_merge_rule="match_score_high",
        auto_ignore_rule="match_score_low",
    )
}



class DecisionEngine:
    def __init__(self, client_cfg: ClientConfig | None = None):
        self.client_cfg = client_cfg
        self.profile_name = client_cfg.MATCHING_PROFILE if client_cfg else "default_v1"
        configured_profiles = client_cfg.MATCHING_PROFILES if client_cfg else {}
        self.profile = configured_profiles.get(
            self.profile_name,
            DEFAULT_PROFILES.get(self.profile_name, DEFAULT_PROFILES["default_v1"]),
        )
        if self.profile.auto_ignore_rule is None:
            self.profile.auto_ignore_rule = DEFAULT_PROFILES['default_v1'].auto_ignore_rule
        if self.profile.auto_merge_rule is None:
            self.profile.auto_merge_rule = DEFAULT_PROFILES['default_v1'].auto_merge_rule

        self.upper_bound = client_cfg.BOUNDS.u_bound if client_cfg else 90.0
        self.lower_bound = client_cfg.BOUNDS.l_bound if client_cfg else 45.0
        self.predicate_registry = self._build_predicate_registry()
        assert self.profile.auto_merge_rule
        assert self.profile.auto_ignore_rule
        self._merge_tree = self._compile(self.profile.auto_merge_rule)
        self._ignore_tree = self._compile(self.profile.auto_ignore_rule)

    def _compile(self, expression: str) -> RuleNode:
        operators = list(re.finditer(r"(_+)(and|or)\1", expression))
        if not operators:
            token = expression.strip()
            if not token:
                raise ConfigError("Matching rule contains an empty expression")
            return RuleNode(op='predicate', token=token)
        depth = max(len(match.group(1)) for match in operators)
        outer = [m for m in operators if len(m.group(1)) == depth]

        parts, ops, start = [],[],0
        for match in outer:
            parts.append(expression[start:match.start()])
            ops.append(match.group(2))
            start = match.end()
        parts.append(expression[start:])
        children = [self._compile(p) for p in parts]
        return RuleNode(op=ops[0], children=children)
        
    @staticmethod
    def _rule_name(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")

    def _build_predicate_registry(self) -> dict[str, PredicateSpec]:
        registry: dict[str, PredicateSpec] = {
            "match_score_high": PredicateSpec("overall", "high"),
            "match_score_low": PredicateSpec("overall", "low"),
        }
        if self.client_cfg is None:
            return registry

        for contact_type, config in self.client_cfg.COLUMNS:
            if config is None:
                continue
            column_operators = ("exact",) if contact_type == "phone" else ("exact", "high", "low")
            for column in config.active_columns:
                token = self._rule_name(column)
                for operator in column_operators:
                    registry[f"{token}_{operator}"] = PredicateSpec(
                        "column", operator, column=column
                    )
            group_token = self._rule_name(contact_type)
            if contact_type == "address" and config.combine:
                registry["address_exact"] = PredicateSpec(
                    "column", "exact", column="address_combined"
                )
            if contact_type == "phone":
                group_operators = ("exact", "high")
            elif contact_type == "email":
                group_operators = ("exact", "high", "low")
            else:
                group_operators = ("high", "low")
            for operator in group_operators:
                registry[f"{group_token}_{operator}"] = PredicateSpec(
                    "group", operator, contact_type=contact_type
                )
        return registry
    
    def _group_score(self, evidence: MatchEvidence, contact_type: str) -> float | None:
        category = evidence.categories.get(contact_type)
        return category.aggregate_score if category is not None else None

    def _group_fields(
        self,
        evidence: MatchEvidence,
        contact_type: str,
    ) -> list[FieldEvidence]:
        category = evidence.categories.get(contact_type)
        return list(category.comparisons) if category is not None else []

    @staticmethod
    def _source_name(field_name: str) -> str:
        return field_name.removeprefix("clean_").split(":", 1)[0]

    def _group_exact(self, evidence: MatchEvidence, contact_type: str) -> bool:
        return any(
            field.match_type is MatchType.EXACT
            for field in self._group_fields(evidence, contact_type)
        )

    def _phone_majority_exact(self, evidence: MatchEvidence) -> bool:
        fields = self._group_fields(evidence, "phone")
        if not fields:
            return False
        exact_count = sum(field.match_type is MatchType.EXACT for field in fields)
        return exact_count > len(fields) / 2

    def _column_fields(self, evidence: MatchEvidence, column: str) -> list[FieldEvidence]:
        return [
            field
            for category in evidence.categories.values()
            for field in category.comparisons
            if self._rule_name(self._source_name(field.left_field)) == self._rule_name(column)
            or self._rule_name(self._source_name(field.right_field)) == self._rule_name(column)
        ]

    def _column_score(self, evidence: MatchEvidence, column: str) -> float | None:
        fields = self._column_fields(evidence, column)
        scores = [field.score for field in fields]
        return scores[0] if scores else None

    def _evaluate_predicate(self, name: str, evidence: MatchEvidence) -> bool:
        spec = self.predicate_registry.get(name)
        if spec is None:
            raise ConfigError(f"Rule references unknown predicate {name!r}")

        if spec.kind == "overall":
            score = evidence.overall_score
        elif spec.kind == "group":
            score = self._group_score(evidence, spec.contact_type or "")
        else:
            score = self._column_score(evidence, spec.column or "")

        if spec.kind == "group" and spec.operator == "exact":
            return self._group_exact(evidence, spec.contact_type or "")

        if spec.kind == "group" and spec.contact_type == "phone" and spec.operator == "high":
            return self._phone_majority_exact(evidence)

        if spec.operator == "exact":
            fields = self._column_fields(evidence, spec.column or "")
            return any(field.match_type is MatchType.EXACT for field in fields)
        if score is None:
            return False
        if spec.operator == "high":
            return score >= self.upper_bound
        if spec.operator == "low":
            return score <= self.lower_bound
        raise ConfigError(f"Unsupported predicate operator {spec.operator!r}")

    def _eval_node(self, node: RuleNode, evidence: MatchEvidence):
        if node.op == 'predicate':
            assert node.token
            return self._evaluate_predicate(node.token, evidence)
        elif node.op == 'and':
            assert node.children
            return all(self._eval_node(child, evidence) for child in node.children)
        elif node.op == 'or':
            assert node.children
            return any(self._eval_node(child, evidence) for child in node.children)
    
    def _evaluate(self, tree: RuleNode, evidence: MatchEvidence):
        return self._eval_node(tree, evidence)

    def _result(
        self,
        evidence: MatchEvidence,
        decision: Decision,
        matched_rule: str | None,
        reason_codes: tuple[str, ...],
        reason: str,
    ) -> PairDecision:
        contact_data = {
            name: tuple(category.comparisons)
            for name, category in evidence.categories.items()
        }
        return PairDecision(
            left_id=evidence.left_id,
            right_id=evidence.right_id,
            decision=decision,
            overall_score=evidence.overall_score,
            name_score=self._group_score(evidence, "name"),
            address_score=self._group_score(evidence, "address"),
            email_score=self._group_score(evidence, "email"),
            phone_score=self._group_score(evidence, "phone"),
            match_types={
                name: tuple(field.match_type for field in category.comparisons)
                for name, category in evidence.categories.items()
            },
            contact_data=contact_data,
            matched_rule=matched_rule,
            reason_codes=reason_codes,
            reason=reason,
            profile=self.profile_name,
            profile_version=self.profile_name,
            evidence=evidence,
        )

    def decide(self, evidence: MatchEvidence) -> PairDecision:
        if not any(category.comparisons for category in evidence.categories.values()):
            return self._result(
                evidence,
                Decision.INSUFFICIENT_DATA,
                None,
                ("NO_COMPARABLE_FIELDS",),
                "No comparable fields were available for this candidate pair.",
            )

        assert self.profile.auto_ignore_rule
        assert self.profile.auto_merge_rule


        if self._evaluate(self._ignore_tree, evidence):
            return self._result(
                evidence, Decision.IGNORE, self.profile.auto_ignore_rule,
                ("AUTO_IGNORE_RULE_MET",),
                f"auto ignore rule: {self.profile.auto_ignore_rule} has been satisfied",
            )
        elif self._evaluate(self._merge_tree, evidence):
            return self._result(
                evidence, Decision.MERGE, self.profile.auto_merge_rule,
                ("AUTO_MERGE_RULE_MET",),
                f"Auto merge rule: {self.profile.auto_merge_rule} has been satisfied",
            )
        else:
            return self._result(
                evidence, Decision.REVIEW, None,
                ("NO_AUTO_RULES_MET",),
                "No auto rules have been satisfied",
            )
    def decide_all(self, evidence: list[MatchEvidence]) -> list[PairDecision]:
        return [self.decide(item) for item in evidence]
