# contact-dedupe

A CLI tool for finding likely duplicate contact records in CRM exports
(Salesforce NPSP and similar). Each run:

1. normalizes/cleans the configured columns,
2. generates candidate duplicate pairs using configurable blocking strategies,
3. builds field-level evidence (exact / fuzzy / nickname matches and scores) for every pair,
4. applies a configurable matching profile to assign each pair a decision, and
5. writes a review CSV plus a run summary JSON.

Decisions are **pair-level** only: each pair is labeled `MERGE`, `REVIEW`,
`IGNORE`, or `INSUFFICIENT_DATA`. The pipeline does **not** currently write a
merged file or a `duplicate_groups.csv`; a `DuplicateGrouper` helper exists in
the package but is not yet wired into the run/output pipeline (see
[Development status](#development-status)).

## Requirements

- Python 3.14+ (managed automatically by `uv` if not already installed)
- [uv](https://docs.astral.sh/uv/)

## Installation

```bash
git clone https://github.com/emccloud15/contact-dedupe
cd contact-dedupe
uv sync
```

`uv sync` creates a `.venv`, installs the dependencies declared in
`pyproject.toml`, and installs `contact-dedupe` itself (editable) as the
`contact-dedupe` console script.

## Usage

```bash
uv run contact-dedupe --dir path/to/directory
```

Or activate the environment once instead of prefixing every command:

```bash
uv sync
source .venv/bin/activate
contact-dedupe --dir path/to/directory
```

### CLI options

All options are optional; anything omitted is requested interactively.

| Option | Meaning |
|---|---|
| `--dir <path>` | Directory containing exactly one `.yaml` config and one `.csv` data file (see below). When given, `--yaml` and `--file` are not used. |
| `--yaml <path>` | Path to the client YAML config file. Must already exist. |
| `--file <path>` | Path to the data file to dedupe. Must already exist. |
| `--output <path>` | Output directory. Created if missing. |

Path values are stripped of surrounding single/double quotes, so a path pasted
with quotes still resolves.

### Interactive prompts and the file-picker fallback

For any value not supplied on the command line, a native OS picker (tkinter) is
opened. If tkinter is unavailable or the picker cannot be shown, the tool logs a
warning and falls back to a terminal path prompt (`questionary.path`). If nothing
is selected, the run aborts with `No file selected.`

### Input directory behavior (`--dir`)

`--dir` lists the directory and ignores dot-files (e.g. `.DS_Store`). It then
requires exactly **two** files:

- one file whose extension is exactly `.yaml`, and
- one file whose extension is exactly `.csv`.

Any other file count raises `DataLoadError` ("There must be only two files…"),
and two files that are not one of each type raise `DataLoadError` ("There must be
a .yaml file and a .csv file…"). Note that `--dir` accepts a `.csv` only — an
`.xlsx` data file must be supplied via `--file` (see below).

## Data cleaning & normalization

Before matching, the configured columns are validated to exist in the dataframe
and then cleaned (`contact_dedupe/dedupe/cleaning.py`), per contact type:

| Type | Cleaning |
|---|---|
| `name` | Removes every non-`A–Z`/`a–z` character, lowercases |
| `email` | Lowercases, strips whitespace, removes internal spaces |
| `phone` | Keeps digits only; drops a leading US country code `1` when the result is 11 digits |
| `address` | Truncates at the first `-`, then strips whitespace/quotes/commas/periods, lowercases |

Cleaned values that are blank or null stay null (they are not treated as an
empty-string match). The normalizer (`contact_dedupe/dedupe/normalize.py`) then
builds internal columns:

- One cleaned column per configured source column, named `clean_<source
  column>:<contact type>` (for example `clean_Phone:phone`).
- When a contact type has `combine`, the cleaned source columns are stripped and
  **concatenated with no separator** into a single column named
  `clean_<type>_combined:<type>` (for example `clean_address_combined:address`).
  Null/empty values are treated as empty strings, so the combined value joins only
  the values that are present and is null only when every combined field is empty.
- `include_name: true` on a contact type produces an additional
  `clean_<...>:name_<type>` column whose value is the cleaned value with the
  cleaned name column(s) **concatenated onto it (no separator)**. The schema
  permits omitting a `name` section, but using `include_name: true` requires a
  `name` section to be configured so the cleaned name column(s) exist for
  normalization to append.

A stable internal record id column `_record_id` is appended. It uses the
`MATCH_FIELD` values (prefixed `record:`) when those are non-null and unique;
otherwise it falls back to positional ids (`record:0`, `record:1`, …). Dataframe
index labels are never used as identity.

## Candidate blocks

Candidate pairs are produced by blocking strategies in
`contact_dedupe/dedupe/candidate_generator.py`. Two `ClientConfig` fields control
this:

- `CANDIDATE_BLOCK_PROFILE` (string, default `default_v1`) selects a named list.
- `CANDIDATE_BLOCKS` (mapping of profile name → list of block definitions) holds
  the definitions. If the selected profile is not present there, the built-in
  `default_v1` blocks are used.

Each block is one of three types:

| `type` | Required keys | Behavior |
|---|---|---|
| `exact` | `field` | Buckets records by the full cleaned value of `field`. |
| `prefix` | `field`, `length`, optional `direction` (`start`/`end`) | Buckets by the first/last `length` characters of `field`. |
| `composite` | `fields` (≥ 2) | Buckets by the configured fields joined with `\|`. |
| any | optional `max_bucket_size` (default `1000`, minimum `2`) | Buckets larger than this are skipped entirely. |

Records with null/blank keys are excluded from that block. A pair present in
multiple blocks records every block strategy that produced it (provenance in
`candidate_blocks`).

The built-in `default_v1` profile is:

```yaml
CANDIDATE_BLOCK_PROFILE: default_v1
CANDIDATE_BLOCKS:
  default_v1:
    - {type: exact, field: email, max_bucket_size: 500}
    - {type: exact, field: phone, max_bucket_size: 500}
    - {type: exact, field: postal_code, max_bucket_size: 1000}
    - {type: composite, fields: [last_name, postal_code], max_bucket_size: 500}
```

A block `field` is resolved against the normalized dataframe: an exact column
name, then any `*:<field>` column, then any `clean_<field>…` column, then a
tokenized substring match. This is why logical names such as `email`, `phone`,
`last_name`, or `postal_code` can be used, provided a matching normalized column
exists (otherwise a `ConfigError` is raised).

> **Note:** the shipped `example_config.yaml` is aligned with the recognized
> `CANDIDATE_BLOCKS` schema. It defines an `acme_v1` profile whose blocks
> (`exact` email, `exact` phone, `exact` ZIP, and a `composite` of last name +
> ZIP) all resolve against the example source columns. Custom blocks belong
> under `CANDIDATE_BLOCKS`.

## Matching profiles, rules, and decisions

`contact_dedupe/dedupe/evidence.py` compares every candidate pair field by field.
For `phone` and `email` categories it compares every left column against every
right column (a Cartesian product of the available columns); for `name` and
`address` it compares same-named columns. Each comparison yields a `MatchType`:

- `EXACT` — cleaned values are identical,
- `FUZZY` — `rapidfuzz` `WRatio` score is above `BOUNDS.l_bound` (phones are
  never fuzzy-scored; they are exact or nothing),
- `NICKNAME` — intended for a non-exact name pair whose values are nicknames of
  each other (only when `NICKNAME` is configured), but in the current code such a
  pair is promoted to score `100`, which the `FUZZY` branch above already catches,
  so the emitted match type is `FUZZY` rather than `NICKNAME` (the `NICKNAME`
  branch is effectively unreachable), or
- `NO_MATCH` — everything else.

A category's score is the **plain mean** of its field scores. A pair's
`overall_score` is the sum of the category scores **divided by 4** (a fixed
divisor), so with fewer than four configured contact types the maximum possible
score is below 100.

> Weight behavior: `COLUMNS.<type>.weight` is validated and stored, and
> `ClientConfig.weight_for` exposes a resolved per-column weight, but the current
> evidence aggregation and overall score do **not** apply weights. See
> [Deprecated configuration fields retained for compatibility](#deprecated-configuration-fields-retained-for-compatibility).

`contact_dedupe/dedupe/decision.py` selects a profile via `MATCHING_PROFILE` from
the `MATCHING_PROFILES` mapping (default profile `default_v1`, which uses
`match_score_high` / `match_score_low`). Each profile provides two rules:

```yaml
MATCHING_PROFILE: balanced_v1
MATCHING_PROFILES:
  balanced_v1:
    auto_merge_rule: email_high_or_phone_high
    auto_ignore_rule: match_score_low
```

Rule grammar: an expression is either a single predicate token, or several
predicates joined by `_and_` / `_or_`. Nesting is expressed by using **more
underscores** on the outer operator, for example
`a_or_b__and__c_or_d` (single-underscore operators bind first). Predicate tokens
available for the configured columns are:

- `match_score_high`, `match_score_low` — overall score vs. `u_bound` / `l_bound`.
- `<column token>_exact`, `<column token>_high`, `<column token>_low` — per
  source column (tokenized from the column name). Phones support `_exact` only.
- `<contact type>_exact`, `<contact type>_high`, `<contact type>_low` — the
  category (phone supports `exact`/`high`; email `exact`/`high`/`low`; name and
  address `high`/`low`).
- `address_exact` — requires an `address` section with `combine`.

Evaluation thresholds: `_high` is `score >= BOUNDS.u_bound`, `_low` is
`score <= BOUNDS.l_bound`, `_exact` is an exact field match. For the phone group,
`_high` means a strict majority of compared phone fields are exact.

Decisions are evaluated in this order:

1. `INSUFFICIENT_DATA` — no comparable fields for the pair.
2. `IGNORE` — the `auto_ignore_rule` matches.
3. `MERGE` — the `auto_merge_rule` matches.
4. `REVIEW` — neither rule matches.

Note the decisions are `MERGE`, `REVIEW`, `IGNORE`, and `INSUFFICIENT_DATA`.
There are no `AUTO_MERGE` / `NOT_DUPLICATE` decision values.

## Client config (YAML)

Configs are validated by the `ClientConfig` Pydantic model
(`contact_dedupe/common/models.py`). Recognized top-level fields:

| Field | Type / default | Notes |
|---|---|---|
| `CLIENT_NAME` | string, required | Used in the output folder name. |
| `COLUMNS` | mapping, required | At least one of `phone`/`email`/`address`/`name` must be present. |
| `MAIN_MATCH_CRITERIA` | string, required | Must be `address` or one of the configured `columns` entries (the model does not accept `combine` entries). Required/validated but not used to gate candidates or decisions (see [Deprecated configuration fields retained for compatibility](#deprecated-configuration-fields-retained-for-compatibility)). |
| `MATCH_FIELD` | string, required | Non-null/unique values become the internal record ids and the ids shown in outputs. |
| `BOUNDS` | mapping, required | `u_bound` (default `90.0`) and `l_bound` (default `75.0`), each 0–100 with `l_bound <= u_bound`. |
| `NICKNAME` | string, optional | Source column used for nickname-aware name matching. |
| `EXCLUSION` | `{column: <name>}`, optional | Column is validated to exist; it is not otherwise used by the current pipeline (deprecated). |
| `CANDIDATE_BLOCK_PROFILE` | string, default `default_v1` | Selects the block list. |
| `CANDIDATE_BLOCKS` | mapping, default `{}` | Profile name → list of block definitions. |
| `MATCHING_PROFILE` | string, default `default_v1` | Must exist in `MATCHING_PROFILES` when that mapping is non-empty. |
| `MATCHING_PROFILES` | mapping, default `{}` | Profile name → `{auto_merge_rule, auto_ignore_rule}`. |
| `BASE` | bool, default `False` | Accepted but not used (deprecated). |
| `ADDRESS` | bool, default `False` | Accepted but not used (deprecated). |
| `STRICT_MATCH` | bool, default `False` | Accepted but not used (deprecated). |

Each `COLUMNS.<type>` entry (`phone`, `email`, `address`, `name`):

| Key | Type / default | Notes |
|---|---|---|
| `columns` | list of strings | Source column name(s) fed to that contact type. |
| `combine` | list of strings | Source column(s) concatenated into one value (needs ≥ 2 entries). |
| `include_name` | bool, default `False` | Concatenate the cleaned name onto the cleaned value. |
| `weight` | float or list of `[column, weight]` pairs, default `0.0` | Validated to be within 0–1; see [Deprecated configuration fields retained for compatibility](#deprecated-configuration-fields-retained-for-compatibility). |

At least one of `columns`/`combine` must be non-empty per configured type, and
`combine` must list two or more fields. A single float `weight` is split evenly
across that type's active columns.

### Weights prompt

If any configured column has no positive weight, or the weights do not total
`1.0`, the CLI warns and asks "Some contact columns have no weight. Auto-balance
them?" Choosing yes proportionally normalizes all weights to sum to 1.0; choosing
no aborts with `ConfigError`. Because weights are not applied to scoring (see
[Deprecated configuration fields retained for compatibility](#deprecated-configuration-fields-retained-for-compatibility)),
this only changes the stored weights — it does not change field evidence or
decisions.

### Example

```yaml
CLIENT_NAME: acme_contacts

COLUMNS:
  name:
    include_name: false
    weight:
      - [First Name, 0.15]
      - [Last Name, 0.15]
    columns: [First Name, Last Name]
  email:
    weight: 0.25
    columns: [Email, Alternate Email]
  phone:
    weight: 0.20
    columns: [Phone, Mobile Phone]
  address:
    weight: 0.25
    # `columns` keeps a normalized ZIP field for candidate blocking; `combine`
    # needs at least two fields and its value is what `address_exact` matches.
    columns: [ZIP]
    combine: [Street, City, State]

CANDIDATE_BLOCK_PROFILE: acme_v1
CANDIDATE_BLOCKS:
  acme_v1:
    - {type: exact, field: email, max_bucket_size: 500}
    - {type: exact, field: phone, max_bucket_size: 500}
    - {type: exact, field: ZIP, max_bucket_size: 1000}
    - {type: composite, fields: [Last Name, ZIP], max_bucket_size: 500}

MATCHING_PROFILE: balanced_v1
MATCHING_PROFILES:
  balanced_v1:
    auto_merge_rule: email_high_or_phone_high
    auto_ignore_rule: match_score_low

MAIN_MATCH_CRITERIA: Email
MATCH_FIELD: Contact ID
NICKNAME: First Name

BOUNDS:
  u_bound: 90
  l_bound: 75
```

### Deprecated configuration fields retained for compatibility

The following fields are still accepted and validated by `ClientConfig`, and
**should not be removed yet**, but they are **not** active matching controls:

- `MAIN_MATCH_CRITERIA` — required and validated (it must be `address` or one of
  the configured `columns` entries; the model does not accept `combine` entries),
  but it is **not** used to gate candidates or decisions in the current code.
- `COLUMNS.<type>.weight` — validated and stored (`ClientConfig.weight_for`
  resolves a per-column weight) and it can trigger the CLI auto-balance prompt,
  but it is **not** applied to field evidence or the category/overall scores.
- `EXCLUSION` — the configured column is validated and required in the input, but
  no exclusion behavior is applied.
- `BASE`, `ADDRESS`, `STRICT_MATCH` — accepted but currently unused.

`COLUMNS` itself and the `address` contact type are **active**; only the fields
listed above are deprecated. `example_config.yaml` keeps `MAIN_MATCH_CRITERIA`
and the `COLUMNS` weights as active settings (so the copy-ready file stays valid)
and comments out `EXCLUSION`, `BASE`, `ADDRESS`, and `STRICT_MATCH`.

## Output

Outputs are written (creating the directory if needed) to

```
<output dir>/
```

containing:

### `dedupe_review_<client_name>_<todays date>.csv`

One row per candidate pair, columns in this order:

| Column | Meaning |
|---|---|
| `left_id`, `right_id` | Original `MATCH_FIELD` values of the two records. |
| `decision` | `MERGE` / `REVIEW` / `IGNORE` / `INSUFFICIENT_DATA`. |
| `overall_score` | Sum of category scores ÷ 4. |
| `name_score`, `address_score`, `email_score`, `phone_score` | Per-category mean scores. |
| `left_<original columns>`, `right_<original columns>` | The full original values of both records. |
| `name_match_details`, `address_match_details`, `email_match_details`, `phone_match_details` | Per-field `Left -> Right: MATCH_TYPE` strings (fuzzy scores included). |
| `reason` | Human-readable explanation. |
| `matched_rule` | The rule that fired, or empty. |

### `run_summary_<client_name>_<todays date>.json`

JSON with `input_record_count`, `candidate_pair_count`, `decision_counts`,
`pair_details` (per pair: `left_id`, `right_id`, `decision`, `candidate_blocks`,
and `contact_data` field-level comparisons), `profile_used`, `profile_version`,
and an ISO-8601 UTC `run_timestamp`.

No other artifacts (for example `duplicate_groups.csv`) are produced by the
current writer (`contact_dedupe/dedupe/writer.py`).

## Python API

```python
from contact_dedupe.common.utils import Utilities
from contact_dedupe.dedupe import Dedupe

cfg = Utilities.load_client_config(yaml_path)
df = Utilities.load_data_df(data_path)

pipeline = Dedupe(client_cfg=cfg, df=df)
review = pipeline.run()                 # returns the review DataFrame (one row per candidate pair)
artifacts = pipeline.write_outputs(out) # {"review": Path, "summary": Path}
```

The `contact_dedupe.dedupe` package re-exports `Dedupe`, `CandidateGenerator`,
`EvidenceBuilder`, `FieldEvidence`, `MatchEvidence`, `Decision`, `DecisionEngine`,
`PairDecision`, `DuplicateGroup`, `DuplicateGrouper`, `GroupingResult`,
`ResultWriter`, and `evaluate_decisions`.

`evaluate_decisions(decisions, duplicate_pairs)` scores a list of `PairDecision`
objects against a set of known duplicate pairs (`frozenset` of record-id pairs),
returning `candidate_pair_count`, `candidate_recall`, `pair_precision`,
`pair_recall`, `automatic_merge_precision`, and `review_rate`.

Data loading: `.csv` files load via `pandas.read_csv`; any other extension loads
via `pandas.read_excel(..., engine="openpyxl")`. **`openpyxl` is not a declared
dependency**, so reading `.xlsx` requires installing it separately; CSV runs need
nothing extra.

## Troubleshooting

| Error / message | Cause |
|---|---|
| `DataLoadError: There must be only two files…` | `--dir` contains more or fewer than 2 non-hidden files. |
| `DataLoadError: There must be a .yaml file and a .csv file…` | `--dir` has 2 files but not one of each required type. |
| `DataLoadError: File path is not a yaml file…` | `--yaml` was given a non-`.yaml` file. |
| `DataLoadError: Failed to load csv / xlsx data file…` | The data file could not be read (for `.xlsx`, often a missing `openpyxl`). |
| `ConfigError: Client config must contain a YAML mapping…` | The YAML root is not a mapping. |
| `ConfigError: Invalid client configuration in …` | Pydantic validation failed — check field names/types against the schema above. |
| `ConfigError: The MAIN_MATCH_CRITERIA value must be one of […]` | `MAIN_MATCH_CRITERIA` is neither `address` nor one of the configured `columns` entries. |
| `ConfigError: At least one contact field must be configured in COLUMNS.` | No `phone`/`email`/`address`/`name` section was provided. |
| `ConfigError: MATCHING_PROFILE … is not defined in MATCHING_PROFILES` | Selected profile is missing from the mapping. |
| `ConfigError: Both 'columns' and 'combine' can not be empty for field: …` | A configured contact type has neither. |
| `ConfigError: To use the 'combine' setting for: …` | `combine` lists fewer than two fields. |
| `ConfigError: Configured columns are missing from the dataframe: …` | A configured column is not present in the input file. |
| `ConfigError: Candidate block field … is not in the normalized dataframe` | A block `field` could not be resolved to a normalized column. |
| `ConfigError: Rule references unknown predicate …` | A matching rule uses a token that was not registered. |
| `UserWarning` about rule-name collisions / address `combine` | A column name tokenizes to a reserved contact-group name, or `address` lacks `combine` (needed for `address_exact`). |
| Auto-balance prompt / `ConfigError: Weight configuration was not auto-balanced; exiting.` | Weights are missing or do not sum to 1.0. |

Logging goes to stdout and to a `dedupe.log` file in the working directory.

## Development status

- Tests live in `tests/` and are configured via `[tool.pytest.ini_options]` in
  `pyproject.toml`. They are **stale** relative to the current code: collection
  fails because `tests/test_coverage_extra.py` imports a `Blocking` name that no
  longer exists in `contact_dedupe.common.models`. As a result `uv run pytest`
  currently stops with a collection error and the suite cannot be reported as
  passing. Several tests also reference legacy grouping APIs (for example
  `Dedupe.grouping`, `DuplicateGrouper`) that the current pipeline does not use.
- `contact_dedupe/common/final_files.py` and `contact_dedupe/dedupe/dsu.py` are
  legacy modules not used by the current pipeline.
- Type checking is configured for `pyright` over `contact_dedupe/`.
