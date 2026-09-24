import pandas as pd

from contact_dedupe.dedupe.evidence import EvidenceBuilder


def test_evidence_records_scores_missing_fields_and_identifier_conflicts():
    normalized = pd.DataFrame(
        {
            "_record_id": ["record:A", "record:B"],
            "clean_email:email": ["a@example.com", "other@example.com"],
            "clean_phone:phone": ["5551234", "5551234"],
            "clean_name:name": ["maria", pd.NA],
        }
    )
    candidates = pd.DataFrame(
        [
            {
                "left_record_id": "record:A",
                "right_record_id": "record:B",
                "candidate_blocks": ("exact:phone:phone",),
            }
        ]
    )

    evidence = EvidenceBuilder().build_all(normalized, candidates)[0]

    assert evidence.left_id == "record:A"
    assert evidence.fields["clean_phone:phone"].exact is True
    assert evidence.fields["clean_email:email"].exact is False
    assert evidence.conflicts == []
    assert "name" in evidence.missing_fields
    assert evidence.candidate_blocks == ("exact:phone:phone",)
    assert evidence.match_score is not None


def test_evidence_uses_nickname_aware_name_matching():
    normalized = pd.DataFrame(
        {
            "_record_id": ["record:A", "record:B"],
            "clean_First Name:name": ["robert", "bob"],
        }
    )

    from types import SimpleNamespace

    evidence = EvidenceBuilder(
        SimpleNamespace(NICKNAME="First Name", weight_for=lambda *_: 1.0)
    ).build_pair(normalized, "record:A", "record:B")

    assert evidence.fields["clean_First Name:name"].exact is False
    assert evidence.fields["clean_First Name:name"].score == 100.0
    assert evidence.fields["clean_First Name:name"].match_type.value == "NICKNAME"
