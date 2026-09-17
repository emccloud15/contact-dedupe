# Contact Dedupe Refactor Plan

## Goal

Refactor the tool into a configurable, explainable deduplication pipeline that can support different client matching policies while preserving the Python input API:

```python
dedupe = Dedupe(client_cfg=config, df=dataframe)
result = dedupe.run()
```

The output structure is allowed to change. Existing downstream programs will be updated to use the new `ClientConfig` and result format.

## Design principles

- Candidate generation and duplicate decisions are separate concerns.
- Blocking should optimize performance and recall; it must not determine whether a pair is a duplicate.
- Matching policies should be named, versioned, validated, and selected through configuration.
- Every decision should be explainable through field-level evidence and reason codes.
- Missing values must never count as matching values.
- Strong identifier conflicts must be represented explicitly and should prevent unsafe automatic merges.
- Duplicate grouping happens after pair-level decisions.
- The system should default to review-oriented behavior; automatic merging must require strong evidence.

## Target pipeline

```text
load raw data and configuration
        |
        v
validate input schema and configuration
        |
        v
normalize records while retaining original values
        |
        v
generate candidate pairs from multiple blocking strategies
        |
        v
compare each candidate pair and produce field-level evidence
        |
        v
apply the selected matching profile
        |
        v
classify each pair
        |
        v
build duplicate groups from accepted relationships
        |
        v
write client-facing results and run metadata
```

## Compatibility boundary

The `Dedupe` constructor and `run()` entry point remain stable:

```python
Dedupe(client_cfg=config, df=dataframe).run()
```

Internally, `Dedupe` should become a thin facade over the new pipeline. The result dataframe and its columns may change.

The current configuration model may be expanded or replaced. Existing callers can update their configuration construction to the new `ClientConfig` shape.

## Proposed internal components

### `Normalizer`

Responsibilities:

- Clean configured fields.
- Preserve nulls and blanks as missing values.
- Retain original input columns.
- Assign a stable internal record ID independent of dataframe index.
- Produce normalized fields used by blocking and comparison.

The existing cleaning functions should remain small stateless functions where practical. Normalization should be tested independently from matching.

### `CandidateGenerator`

Responsibilities:

- Build candidate pairs from multiple configured blocking strategies.
- Union and deduplicate pairs from all blocks.
- Exclude null/blank blocking keys.
- Guard against oversized buckets caused by common or invalid values.
- Record which blocking strategies generated each candidate pair.

Candidate blocks should be derived from normalized fields. Examples include:

- exact normalized email;
- exact normalized phone;
- postal-code prefix;
- surname prefix or phonetic surname;
- address number;
- composite surname plus postal code.

Candidate generation affects recall: a pair outside every candidate block will not reach final comparison.

### `EvidenceBuilder`

Responsibilities:

- Compare each candidate pair using all available configured fields.
- Produce field-level scores and exact-match indicators.
- Detect conflicts in strong identifiers.
- Record missing fields and the fields actually used.
- Support nickname-aware name comparison through normalized internal field names.

Possible runtime objects:

```python
FieldEvidence(
    field="email",
    score=100.0,
    exact=True,
    available=True,
)
```

```python
MatchEvidence(
    left_id="123",
    right_id="456",
    fields={...},
    matched_fields=["email", "name"],
    conflicts=[],
    candidate_blocks=["exact_email"],
)
```

Matrix/vector operations may still be used internally for performance, but they should be an implementation detail of comparison rather than also controlling blocking, policy, and grouping.

### `DecisionEngine`

Responsibilities:

- Apply a selected matching profile to `MatchEvidence`.
- Return a clear pair-level classification:

  - `AUTO_MERGE`;
  - `REVIEW`;
  - `NOT_DUPLICATE`;
  - `INSUFFICIENT_DATA`.

- Return the rule that matched and human-readable reason codes.

The decision engine affects precision and recall after a pair has been generated.

### `DuplicateGrouper`

Responsibilities:

- Build groups from pair-level decisions.
- Use stable record IDs rather than dataframe positions.
- Preserve pair evidence inside or alongside each group.
- Detect ambiguous or conflicting connected components.
- Avoid treating a weak transitive chain as an automatic merge without additional safeguards.

### `ResultWriter`

Responsibilities:

- Produce stable client-facing files.
- Keep internal normalized/helper columns out of the primary review file unless explicitly requested.
- Include profile and run metadata for reproducibility.

## Configuration design

The YAML should select a versioned profile and configure candidate blocks separately from decision rules:

```yaml
CLIENT_NAME: acme
MATCHING_PROFILE: balanced_v1

FIELDS:
  name:
    columns: ["First Name", "Last Name"]
  email:
    columns: ["Email"]
  phone:
    columns: ["Phone"]
  address:
    columns: ["Street", "City", "State", "ZIP"]

CANDIDATE_BLOCKS:
  - type: exact
    field: email
  - type: exact
    field: phone
  - type: prefix
    field: postal_code
    length: 3
  - type: composite
    fields: [last_name, postal_code]

MATCHING_PROFILES:
  strict_v1:
    auto_merge_rules:
      - email_exact_and_phone_exact
    review_rules:
      - name_high_and_address_high

  balanced_v1:
    auto_merge_rules:
      - email_exact
      - phone_exact_and_name_high
    review_rules:
      - name_high_and_address_high
```

Profiles should use a finite set of validated predicates and operators rather than arbitrary Python expressions or function names in YAML.

Profiles should be immutable once used in production. A changed policy should receive a new version such as `balanced_v2` so results remain reproducible.

Configuration validation should cover:

- required fields and supported field types;
- configured CSV columns;
- supported block types and fields;
- valid thresholds and weight ranges;
- valid profile names and rule names;
- compatible combinations of fields and rules.

## Client-facing outputs

### `dedupe_review.csv`

One row per candidate pair, containing:

```text
decision
group_id
primary_record_id
duplicate_record_id
match_score
name_score
email_score
phone_score
address_score
matched_fields
conflicts
reason
profile
profile_version
candidate_blocks
```

Original values should be included with clear `primary_` and `duplicate_` prefixes so the client can review a decision without joining another file.

### `duplicate_groups.csv`

One row per group, containing:

```text
group_id
canonical_record_id
member_count
decision
member_record_ids
group_conflicts
```

### `run_summary.json`

Include:

```text
input_record_count
candidate_pair_count
decision_counts
duplicate_group_count
profile_used
profile_version
run_timestamp
```

## Refactor sequence

1. Complete the test baseline and add regression tests for current cleaning, normalization, configuration, and dedupe behavior.
2. Fix normalization correctness issues, beginning with missing-value handling.
3. Introduce stable record IDs and remove algorithmic dependence on dataframe indexes.
4. Extract candidate generation from the current `Dedupe` implementation while retaining current comparison behavior.
5. Add multiple blocking strategies and candidate-pair provenance.
6. Extract field comparators and evidence objects from the matrix-scoring code.
7. Add the versioned decision-profile model and decision engine.
8. Extract duplicate grouping and add safeguards for transitive and conflicting groups.
9. Replace the current output formatter with the review, group, and summary outputs.
10. Update downstream programs to construct the new `ClientConfig` and consume the new results.
11. Add labeled test fixtures and measure candidate recall, auto-merge precision, and review volume for each profile.

## Testing strategy

Each stage should be testable independently.

### Unit tests

- Cleaning and null handling.
- Normalized field construction.
- Individual blocking strategies.
- Candidate-pair union and deduplication.
- Field comparators.
- Evidence aggregation.
- Profile rule evaluation.
- Stable-ID grouping.

### Integration tests

Cover:

- obvious duplicates;
- obvious non-duplicates;
- same-name records with different contact details;
- shared household addresses;
- missing fields;
- conflicting email or phone values;
- duplicates found only by different blocking passes;
- records in different single-pass blocks;
- transitive A-B-C chains;
- sparse versus complete records;
- each matching profile.

### Evaluation metrics

For labeled fixtures, track:

- candidate recall: percentage of known duplicate pairs generated as candidates;
- pair-level precision and recall;
- automatic-merge precision;
- review rate;
- group-level false merges.

No profile should be considered production-ready based only on aggregate score thresholds or unit-test coverage.
