import pandas as pd

from contact_dedupe.common.models import CandidateBlock, ClientConfig
from contact_dedupe.dedupe.candidate_generator import BlockGenerator, CandidateGenerator
from contact_dedupe.dedupe.normalize import add_record_ids


def test_block_generator_exact_keys_exclude_missing_values():
    normalized = pd.DataFrame({"email": [" a@example.com ", "", None]})

    block_name, keys = next(
        BlockGenerator.generate(normalized, CandidateBlock(type="exact", field="email"))
    )

    assert block_name == "exact:email:email"
    assert keys.iloc[0] == " a@example.com "
    assert keys.iloc[1:].isna().all()


def test_block_generator_prefix_supports_start_and_end():
    normalized = pd.DataFrame({"postal_code": ["12345", "98765", None]})

    _, start_keys = next(
        BlockGenerator.generate(
            normalized,
            CandidateBlock(type="prefix", field="postal_code", length=3),
        )
    )
    _, end_keys = next(
        BlockGenerator.generate(
            normalized,
            CandidateBlock(
                type="prefix", field="postal_code", length=3, direction="end"
            ),
        )
    )

    assert start_keys.iloc[:2].tolist() == ["123", "987"]
    assert end_keys.iloc[:2].tolist() == ["345", "765"]
    assert start_keys.iloc[2:].isna().all()
    assert end_keys.iloc[2:].isna().all()


def test_block_generator_composite_requires_all_fields():
    normalized = pd.DataFrame(
        {"last_name": [" Smith ", "Jones", "Taylor"], "postal_code": ["12345", None, " 60601 "]}
    )

    block_name, keys = next(
        BlockGenerator.generate(
            normalized,
            CandidateBlock(type="composite", fields=["last_name", "postal_code"]),
        )
    )

    assert block_name == "composite:last_name+postal_code"
    assert keys.iloc[[0, 2]].tolist() == ["Smith|12345", "Taylor|60601"]
    assert pd.isna(keys.iloc[1])


def test_candidate_generator_uses_legacy_blocking_when_blocks_are_not_configured():
    config = ClientConfig.model_validate(
        {
            "CLIENT_NAME": "test",
            "COLUMNS": {"name": {"columns": ["Name"]}},
            "BLOCKING": {
                "strict": False,
                "type": "zipcode",
                "column": "ZIP",
                "portion": "end",
            },
            "MAIN_MATCH_CRITERIA": "Name",
            "MATCH_FIELD": "Name",
            "BOUNDS": {},
        }
    )

    generator = CandidateGenerator.from_config(config)

    assert generator.blocks == [
        CandidateBlock(type="prefix", field="ZIP", length=3, direction="end")
    ]


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
