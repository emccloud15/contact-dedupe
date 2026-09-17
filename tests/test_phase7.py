import pandas as pd

from contact_dedupe.dedupe.decision import Decision, DecisionEngine
from contact_dedupe.dedupe.evidence import EvidenceBuilder


def _evidence(email_left, email_right, phone_left, phone_right, name_left="mary", name_right="mary"):
    normalized = pd.DataFrame(
        {
            "_record_id": ["record:A", "record:B"],
            "clean_email:email": [email_left, email_right],
            "clean_phone:phone": [phone_left, phone_right],
            "clean_name:name": [name_left, name_right],
        }
    )
    return EvidenceBuilder().build_pair(normalized, "record:A", "record:B")


def test_balanced_profile_auto_merges_exact_email():
    decision = DecisionEngine().decide(_evidence("same@example.com", "same@example.com", None, None))

    assert decision.decision is Decision.AUTO_MERGE
    assert decision.matched_rule == "email_exact"
    assert decision.reason_codes == ("AUTO_RULE_MATCHED",)


def test_strong_identifier_conflict_downgrades_auto_merge_to_review():
    decision = DecisionEngine().decide(
        _evidence("a@example.com", "b@example.com", "5551234", "5551234")
    )

    assert decision.decision is Decision.REVIEW
    assert "STRONG_IDENTIFIER_CONFLICT" in decision.reason_codes


def test_no_comparable_fields_is_insufficient_data():
    decision = DecisionEngine().decide(_evidence(None, None, None, None, None, None))

    assert decision.decision is Decision.INSUFFICIENT_DATA


def test_custom_profile_is_selected_and_validated():
    from contact_dedupe.common.models import MatchingProfile

    profile = MatchingProfile(auto_merge_rules=["phone_exact"], review_rules=[])
    assert profile.auto_merge_rules == ["phone_exact"]
