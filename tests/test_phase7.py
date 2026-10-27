import pandas as pd
import pytest

from contact_dedupe.common.exceptions import ConfigError
from contact_dedupe.common.models import ClientConfig, MatchingProfile
from contact_dedupe.dedupe.decision import Decision, DecisionEngine
from contact_dedupe.dedupe.evidence import EvidenceBuilder


def _config(**profiles):
    return ClientConfig.model_validate(
        {
            "CLIENT_NAME": "rules",
            "COLUMNS": {
                "name": {"columns": ["First Name", "Last Name"], "weight": 0.25},
                "email": {"columns": ["Email", "Work Email"], "weight": 0.25},
                "phone": {
                    "columns": ["Home Phone", "Work Phone", "Mobile Phone"],
                    "weight": 0.25,
                },
                "address": {"combine": ["Street", "City"], "weight": 0.25},
            },
            "BLOCKING": {"strict": False, "type": "name", "column": "First Name"},
            "MAIN_MATCH_CRITERIA": "First Name",
            "MATCH_FIELD": "First Name",
            "BOUNDS": {"u_bound": 90, "l_bound": 45},
            "MATCHING_PROFILE": "test_v1",
            "MATCHING_PROFILES": {"test_v1": profiles},
        }
    )


def _evidence(config, values):
    normalized = pd.DataFrame({"_record_id": ["record:A", "record:B"], **values})
    return EvidenceBuilder(config).build_pair(normalized, "record:A", "record:B")


def test_exact_email_merges_with_dynamic_profile_rule():
    config = _config(auto_merge_rule="email_exact", auto_ignore_rule="match_score_low")
    evidence = _evidence(config, {"clean_Email:email": ["same@example.com", "same@example.com"]})

    decision = DecisionEngine(config).decide(evidence)

    assert decision.decision is Decision.MERGE
    assert decision.matched_rule == "email_exact"
    assert decision.reason_codes == ("AUTO_MERGE_RULE_MET",)


def test_auto_ignore_overrides_auto_merge():
    config = _config(auto_merge_rule="email_exact", auto_ignore_rule="email_exact")
    evidence = _evidence(config, {"clean_Email:email": ["same@example.com", "same@example.com"]})

    decision = DecisionEngine(config).decide(evidence)

    assert decision.decision is Decision.IGNORE
    assert decision.matched_rule == "email_exact"
    assert decision.reason_codes == ("AUTO_IGNORE_RULE_MET",)


def test_phone_exact_is_any_phone_and_phone_high_is_strict_majority():
    config = _config(auto_merge_rule="phone_high", auto_ignore_rule="phone_exact")
    evidence = _evidence(
        config,
        {
            "clean_Home Phone:phone": ["111", "111"],
            "clean_Work Phone:phone": ["222", "999"],
            "clean_Mobile Phone:phone": ["333", "333"],
        },
    )

    engine = DecisionEngine(config)
    assert engine._matches("phone_exact", evidence) is True
    assert engine._matches("phone_high", evidence) is True
    with pytest.raises(ConfigError, match="unknown predicate"):
        engine._matches("home_phone_high", evidence)


def test_phone_high_fails_when_only_half_are_exact():
    config = _config(auto_merge_rule="phone_high", auto_ignore_rule="phone_exact")
    evidence = _evidence(
        config,
        {
            "clean_Home Phone:phone": ["111", "111"],
            "clean_Work Phone:phone": ["222", "999"],
        },
    )

    assert DecisionEngine(config)._matches("phone_high", evidence) is False


def test_email_high_uses_weighted_group_score():
    config = _config(auto_merge_rule="email_high", auto_ignore_rule="email_low")
    evidence = _evidence(
        config,
        {
            "clean_Email:email": ["same@example.com", "same@example.com"],
            "clean_Work Email:email": ["a@example.com", "b@example.com"],
        },
    )

    assert DecisionEngine(config)._matches("email_exact", evidence) is True
    assert DecisionEngine(config)._matches("email_high", evidence) is True


def test_address_exact_requires_combined_yaml_field():
    config = _config(auto_merge_rule="address_exact", auto_ignore_rule="email_low")
    evidence = _evidence(config, {"clean_address_combined:address": ["123main", "123main"]})

    assert DecisionEngine(config)._matches("address_exact", evidence) is True


def test_nested_rule_expression_is_evaluated():
    config = _config(
        auto_merge_rule="first_name_high_and_last_name_high__or__email_exact",
        auto_ignore_rule="email_low",
    )
    evidence = _evidence(
        config,
        {
            "clean_First Name:name": ["mary", "mary"],
            "clean_Last Name:name": ["bird", "bird"],
            "clean_Email:email": ["a@example.com", "b@example.com"],
        },
    )

    assert DecisionEngine(config)._matches(
        "first_name_high_and_last_name_high__or__email_exact", evidence
    ) is True


def test_unknown_predicate_is_configuration_error():
    config = _config(auto_merge_rule="does_not_exist", auto_ignore_rule="email_low")
    evidence = _evidence(config, {"clean_Email:email": ["a", "b"]})

    with pytest.raises(ConfigError, match="unknown predicate"):
        DecisionEngine(config)._matches("does_not_exist", evidence)


def test_insufficient_data_decision():
    config = _config(auto_merge_rule="email_exact", auto_ignore_rule="email_low")
    evidence = _evidence(config, {"clean_Email:email": [None, None]})

    assert DecisionEngine(config).decide(evidence).decision is Decision.INSUFFICIENT_DATA


def test_matching_profile_uses_singular_rules():
    profile = MatchingProfile(auto_merge_rule="email_exact", auto_ignore_rule="email_low")

    assert profile.auto_merge_rule == "email_exact"
    assert profile.auto_ignore_rule == "email_low"
