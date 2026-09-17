import pandas as pd

from contact_dedupe.common.models import CandidateBlock
from contact_dedupe.dedupe.candidate_generator import CandidateGenerator
from contact_dedupe.dedupe.normalize import add_record_ids


def test_record_ids_use_match_values_and_ignore_dataframe_index():
    source = pd.DataFrame(
        {"Id": ["A", "B"], "email": ["a@example.com", "b@example.com"]},
        index=[10, 42],
    )

    result = add_record_ids(source, source_column="Id")

    assert result["_record_id"].tolist() == ["record:A", "record:B"]
    assert result.index.tolist() == [10, 42]


def test_candidate_blocks_union_pairs_and_preserve_provenance():
    normalized = pd.DataFrame(
        {
            "_record_id": ["record:A", "record:B", "record:C", "record:D"],
            "email": ["same@example.com", "same@example.com", None, "other@example.com"],
            "postal_code": ["12399", "12345", "12300", "99999"],
            "last_name": ["Smith", "Smith", "Smith", "Jones"],
        },
        index=[8, 3, 12, 20],
    )
    generator = CandidateGenerator(
        [
            CandidateBlock(type="exact", field="email"),
            CandidateBlock(type="prefix", field="postal_code", length=3),
            CandidateBlock(type="composite", fields=["last_name", "postal_code"]),
        ]
    )

    candidates = generator.generate(normalized)

    pair = candidates[
        (candidates.left_record_id == "record:A")
        & (candidates.right_record_id == "record:B")
    ].iloc[0]
    assert "exact:email:email" in pair.candidate_blocks
    assert "prefix:postal_code:postal_code" in pair.candidate_blocks
    assert len(candidates) == 3
    assert not ((candidates.left_record_id == "record:C") & (candidates.right_record_id == "record:D")).any()


def test_common_values_are_skipped_when_bucket_exceeds_limit():
    normalized = pd.DataFrame(
        {
            "_record_id": [f"record:{i}" for i in range(3)],
            "email": ["shared@example.com"] * 3,
        }
    )

    candidates = CandidateGenerator(
        [CandidateBlock(type="exact", field="email", max_bucket_size=2)]
    ).generate(normalized)

    assert candidates.empty
