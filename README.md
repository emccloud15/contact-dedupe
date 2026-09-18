# contact-dedupe

A CLI tool for deduplicating CRM contact records (Salesforce NPSP, Virtuous, and similar exports). It runs a two-phase matching pipeline — strict matching on normalized fields followed by weighted fuzzy matching (name, phone, email, address) — and outputs a scored, grouped dedupe file ready for review and merge.

## Requirements

- Python 3.14+ (managed automatically by `uv` if not already installed)
- [uv](https://docs.astral.sh/uv/)

## Installation

```bash
git clone https://github.com/emccloud15/contact-dedupe
cd contact-dedupe
uv sync
```

`uv sync` creates a `.venv`, installs all dependencies from `pyproject.toml`, and installs `contact-dedupe` itself (in editable mode) as a console script.

## Usage

```bash
uv run contact-dedupe --dir path/to/directory
```

Or, activating the environment once instead of prefixing every command:

```bash
uv sync
source .venv/bin/activate
contact-dedupe --dir path/to/directory
```

You'll be prompted interactively:
- **Directory** containing the file to be deduped and its YAML config (if not passed via `--dir`)
- **Is this dedupe file from the Virtuous Data Health tool?** — switches between the standard `Dedupe` pipeline and the `VirtuousDedupe` pipeline, which reshapes Virtuous's side-by-side duplicate export into a stacked format before matching
- If Virtuous: **Strict dedupe on contact type?** — when enabled, records with mismatching `Type`/`Duplicate Type` values are set aside up front, marked `Merge = IGNORE`, and excluded from matching

### Input directory

The `--dir` you provide must contain **exactly two files**:
- One `.yaml` client config file
- One `.csv` file with the contacts to dedupe

Any other file count, or a missing `.yaml`/`.csv`, raises a `DataLoadError` and exits.

### Output

Results are written to a new folder under the selected output directory:

Output_{CLIENT_NAME}_{today's date}/

- `dedupe_review.csv` — one row per candidate pair with the decision, scores, reasons, conflicts, candidate-block provenance, and original values.
- `duplicate_groups.csv` — stable group IDs, canonical records, members, group decisions, and group conflicts.
- `run_summary.json` — input/candidate/decision/group counts, profile information, and a UTC run timestamp.

`example_config.yaml` is a complete copy-ready configuration showing the
available fields, candidate blocks, matching profiles, and optional controls.

## Data cleaning & normalization

Before matching, each configured column is cleaned per contact type (`contact_dedupe/dedupe/cleaning.py`):

| Type | Cleaning |
|---|---|
| `name` | Strips all non-alphabetic characters, lowercases |
| `email` | Lowercases, strips whitespace |
| `phone` | Strips all non-digit characters; drops a leading US country code `1` if the result is 11 digits |
| `address` | Truncates at the first `-` (e.g. dropping suite/apartment suffixes appended with a dash), strips whitespace/quotes/commas/periods, lowercases |

Multiple address columns (street, city, state, zip, etc.) are cleaned individually, then concatenated into a single combined address value used for matching. If `include_name: true` is set for a contact type, the cleaned name is appended to that field's value (pipe-separated) so name is factored into that field's matching.

Rows with a null/blank value for a given field are left out of that field's normalized column entirely, rather than being treated as an empty-string match.

## Client config (YAML)

Each dedupe run is driven by a per-client YAML config validated against a Pydantic schema (`ClientConfig`). Example:

```yaml
CLIENT_NAME: acme-nonprofit

COLUMNS:
  name:
    include_name: true
    weight:
      - ["First Name", 0.15]
      - ["Last Name", 0.15]
    columns:
      - First Name
      - Last Name
  email:
    weight: 0.25
    columns:
      - Email
  phone:
    weight: 0.25
    columns:
      - Phone
      - Mobile Phone
  address:
    weight: 0.20
    columns: ["Mailing Street", "Mailing City", "Mailing State", "Mailing Zip"]

BLOCKING:
  strict: true
  type: zipcode
  column: "Mailing Zip"
  portion: start

MAIN_MATCH_CRITERIA: "Email"
MATCH_FIELD: "Contact ID"
NICKNAME: "First Name"

BOUNDS:
  u_bound: 90.0
  l_bound: 75.0

ADDRESS: false
```

**Field notes:**

| Field | Notes |
|---|---|
| `CLIENT_NAME` | Used in output folder/file naming |
| `COLUMNS.<type>.weight` | A single float (one column) or a list of `[column, weight]` pairs (e.g. separate first/last name weighting). All active weights across every column must sum to `1.0` — if not, you'll be prompted to auto-balance or exit |
| `COLUMNS.<type>.columns` | The actual CSV column name(s) feeding that field type |
| `BLOCKING.type` | One of `zipcode`, `state`, `id`, `name`, `idx` |
| `BLOCKING.portion` | Optional `start` or `end` — blocks on a substring of `BLOCKING.column` (e.g. first 3 digits of a zip) instead of the full value |
| `MAIN_MATCH_CRITERIA` | Must match one of the configured column names; gates whether two records are treated as candidate duplicates, alongside `u_bound` |
| `MATCH_FIELD` | Column used to derive the `match_id` assigned to each duplicate group |
| `NICKNAME` | Optional — enables nickname-aware name matching (e.g. "Christina" / "Tina") via the `nicknames` library, alongside fuzzy string scoring |
| `BOUNDS.u_bound` / `l_bound` | Upper/lower similarity thresholds (0–100) for fuzzy field scoring |
| `ADDRESS` | When `true`, relaxes the strict-dedupe rule that normally requires 3+ matching fields, since address-only comparisons don't have that many comparable columns |

## Troubleshooting

| Error | Cause |
|---|---|
| `DataLoadError: There must be only two files...` | The `--dir` folder has more or fewer than 2 non-hidden files |
| `DataLoadError: There must be a .yaml file and a .csv file...` | Directory has 2 files, but not one of each required type |
| `ConfigError: Invalid client configuration...` | YAML fails Pydantic validation — check for typos or missing required fields against the schema above |
| `ConfigError: The MAIN_MATCH_CRITERIA value must be one of [...]` | `MAIN_MATCH_CRITERIA` doesn't match any configured column name |
| `ConfigError: BLOCKING type ... must be one of [...]` | Invalid `BLOCKING.type` or `BLOCKING.portion` value |
| `ConfigError: Column name is not in the dataframe: {col}` | A column listed in the YAML config doesn't exist in the input CSV |
| `Total fuzzy column weights need to sum to 1.0` | Prompts to auto-balance weights or exit — fix the YAML to avoid the prompt |

## Development

Tests live in `tests/` and run with `pytest` (configured via `[tool.pytest.ini_options]` in `pyproject.toml`):

```bash
uv run pytest
```

Phase 1 establishes baseline coverage for configuration loading, CSV loading,
cleaning, and normalization.

Phase 2 now preserves missing values, validates configured input columns before
normalization, validates matching bounds and weights, and reports actionable
configuration errors. Empty and sparse input frames are covered by regression
tests.

Phases 3–5 add stable internal record IDs and standalone candidate generation
with exact, prefix, and composite blocks. Candidate pairs expose the block
strategies that produced them, while null keys and oversized buckets are
excluded.

Phase 6 adds `EvidenceBuilder`, which compares every generated candidate using
field-level scores, exact-match flags, missing-field tracking, strong-contact
conflicts, nickname-aware name matching, weighted aggregate scores, and
candidate-block provenance. Matching decisions remain the next phase’s scope.

Phase 7 adds versioned matching profiles and finite, validated rules for
`AUTO_MERGE`, `REVIEW`, `NOT_DUPLICATE`, and `INSUFFICIENT_DATA` decisions.
Strong email or phone conflicts always prevent automatic merging and produce an
explainable review reason.

Phases 8–11 make the explainable pipeline active: groups are built only from
automatic pair decisions, transitive components are flagged for review, and
each run writes `dedupe_review.csv`, `duplicate_groups.csv`, and
`run_summary.json`. The CLI accepts `--yaml`, `--file`, and `--output`; profile
evaluation is available through `evaluate_decisions` for labeled fixtures.
