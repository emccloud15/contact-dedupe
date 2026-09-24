"""Versioned, explainable pair-level matching decisions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from contact_dedupe.common.models import ClientConfig, MatchingProfile
from .evidence import MatchEvidence


class Decision(StrEnum):
    AUTO_MERGE = "AUTO_MERGE"
    REVIEW = "REVIEW"
    NOT_DUPLICATE = "NOT_DUPLICATE"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@dataclass(frozen=True)
class PairDecision:
    decision: Decision
    matched_rule: str | None
    reason_codes: tuple[str, ...]
    reason: str
    profile: str
    profile_version: str
    evidence: MatchEvidence


DEFAULT_PROFILES: dict[str, MatchingProfile] = {
    "legacy_v1": MatchingProfile(
        auto_merge_rules=["email_exact", "phone_exact_and_name_high"],
        review_rules=["name_high_and_address_high"],
    ),
    "strict_v1": MatchingProfile(
        auto_merge_rules=["email_exact_and_phone_exact"],
        review_rules=["name_high_and_address_high"],
    ),
    "balanced_v1": MatchingProfile(
        auto_merge_rules=["email_exact", "phone_exact_and_name_high"],
        review_rules=["name_high_and_address_high"],
    ),
    "permissive_v1": MatchingProfile(
        auto_merge_rules=["email_exact", "phone_exact", "phone_exact_and_name_high"],
        review_rules=["name_high_and_address_high"],
    ),
}


class DecisionEngine:
    def __init__(self, client_cfg: ClientConfig | None = None):
        self.client_cfg = client_cfg
        self.profile_name = client_cfg.MATCHING_PROFILE if client_cfg else "balanced_v1"
        configured_profiles = client_cfg.MATCHING_PROFILES if client_cfg else {}
        self.profile = configured_profiles.get(
            self.profile_name,
            DEFAULT_PROFILES.get(self.profile_name, DEFAULT_PROFILES["balanced_v1"]),
        )
        self.upper_bound = client_cfg.BOUNDS.u_bound if client_cfg else 90.0

    @staticmethod
    def _fields(evidence: MatchEvidence, field_type: str):
        return [
            field for column, field in evidence.fields.items()
            if column.rsplit(":", 1)[-1] == field_type
        ]

    @staticmethod
    def _source_name(column: str) -> str:
        return column.removeprefix("clean_").rsplit(":", 1)[0]
    
    def _matches(self, rule: str, evidence: MatchEvidence) -> bool:
        email = self._fields(evidence, "email")
        phone = self._fields(evidence, "phone")
        name = self._fields(evidence, "name")
        address = self._fields(evidence, "address")
        email_exact = any(field.available and field.exact for field in email)
        phone_exact = any(field.available and field.exact for field in phone)
        address_exact = all(field.available and field.exact for field in address)
        first_name_exact = any(field.available and field.exact and self._source_name(field.field).__contains__('First') for field in name)
        last_name_exact = any(field.available and field.exact and self._source_name(field.field).__contains__('Last') for field in name)
        

        first_name_high = any(
            field.available and field.score is not None and self._source_name(field.field).__contains__("First") and field.score >= self.upper_bound
            for field in name
        )
        last_name_high = any(
            field.available and field.score is not None and self._source_name(field.field).__contains__("Last") and field.score >= self.upper_bound
            for field in name
        )
        address_high = any(
            field.available and field.score is not None and field.score >= self.upper_bound
            for field in address
        )
        email_high = any(
            field.available and field.score is not None and field.score >=self.upper_bound
            for field in email
        )


        return {
            "email_exact": email_exact,
            "email_high" : email_high,
            "phone_exact": phone_exact,
            "first_name_exact": first_name_exact,
            "last_name_exact": last_name_exact,
            "first_name_high" : first_name_high,
            "last_name_high" : last_name_high,
            "address_high": address_high,
            "address_exact": address_exact
        }.get(rule, False)

    def decide(self, evidence: MatchEvidence) -> PairDecision:
        if not evidence.used_fields:
            return PairDecision(
                Decision.INSUFFICIENT_DATA,
                None,
                ("NO_COMPARABLE_FIELDS",),
                "No comparable fields were available for this candidate pair.",
                self.profile_name,
                self.profile_name,
                evidence,
            )

        auto_rule = next(
            (rule for rule in self.profile.auto_merge_rules if self._matches(rule, evidence)),
            None,
        )
        if auto_rule:
            if evidence.conflicts:
                return PairDecision(
                    Decision.REVIEW,
                    auto_rule,
                    ("STRONG_IDENTIFIER_CONFLICT",),
                    "A strong identifier conflicts, so automatic merging is unsafe.",
                    self.profile_name,
                    self.profile_name,
                    evidence,
                )
            return PairDecision(
                Decision.AUTO_MERGE,
                auto_rule,
                ("AUTO_RULE_MATCHED",),
                f"Automatic-merge rule {auto_rule!r} matched.",
                self.profile_name,
                self.profile_name,
                evidence,
            )

        review_rule = next(
            (rule for rule in self.profile.review_rules if self._matches(rule, evidence)),
            None,
        )
        if review_rule or evidence.conflicts:
            reason_codes = ("REVIEW_RULE_MATCHED",) if review_rule else ()
            if evidence.conflicts:
                reason_codes += ("STRONG_IDENTIFIER_CONFLICT",)
            return PairDecision(
                Decision.REVIEW,
                review_rule,
                reason_codes,
                "This candidate pair requires human review.",
                self.profile_name,
                self.profile_name,
                evidence,
            )

        return PairDecision(
            Decision.NOT_DUPLICATE,
            None,
            ("NO_MATCHING_RULE",),
            "No matching rule classified this candidate pair as a duplicate.",
            self.profile_name,
            self.profile_name,
            evidence,
        )

    def decide_all(self, evidence: list[MatchEvidence]) -> list[PairDecision]:
        return [self.decide(item) for item in evidence]
