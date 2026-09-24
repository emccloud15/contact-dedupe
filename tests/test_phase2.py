from pathlib import Path

import pandas as pd
import pytest

from contact_dedupe.common.exceptions import ConfigError
from contact_dedupe.common.models import Bounds, ColumnTypeConfig, ClientConfig
from contact_dedupe.common.utils import Utilities
from contact_dedupe.dedupe.cleaning import clean_address, clean_name, clean_phone
from contact_dedupe.dedupe.normalize import normalize_df
from contact_dedupe.dedupe.normalize import combine_fields

CONFIG_PATH = Path(__file__).parent / "data" / "test.yaml"


def test_cleaners_preserve_null_blank_and_non_value_inputs():
    for cleaner in (clean_name, clean_phone, clean_address):
        assert cleaner(None) is None
        assert cleaner(pd.NA) is None
        assert cleaner("   ") is None

    assert clean_phone("not a phone") is None
    assert clean_address("---") is None
    assert clean_name("123-_") is None


def test_normalize_empty_dataframe_is_valid_and_deterministic():
    config = Utilities.load_client_config(CONFIG_PATH)
    source = pd.DataFrame(
        columns=[
            "Primary First Name",
            "Primary Last Name",
            "Primary Address Line 1",
            "Primary Address Line 2",
            "Primary Address City",
            "Primary Address State",
            "Primary Address Postal",
            "Primary Phone Number",
            "Primary Other Phone",
            "Primary Mobile Phone",
            "Primary Work Phone",
            "Primary Email Address",
            "Primary Other Email",
            "Primary Work Email",
        ]
    )

    normalized = normalize_df(
        source,
        config.COLUMNS,
        [field for field, value in config.COLUMNS if value],
    )

    assert normalized.empty
    assert list(normalized.columns[:2]) == ["Primary First Name", "Primary Last Name"]


def test_normalize_rejects_missing_configured_columns():
    config = Utilities.load_client_config(CONFIG_PATH)
    source = pd.DataFrame({"Primary First Name": ["Mary"]})

    with pytest.raises(ConfigError, match="Configured columns are missing"):
        normalize_df(source, config.COLUMNS, ["name"])


def test_normalize_preserves_missing_fields_in_sparse_rows():
    config = Utilities.load_client_config(CONFIG_PATH)
    source = pd.DataFrame(
        {
            "Primary First Name": [pd.NA],
            "Primary Last Name": ["Bird"],
            "Primary Address Line 1": [pd.NA],
            "Primary Address Line 2": [pd.NA],
            "Primary Address City": ["Baton Rouge"],
            "Primary Address State": ["LA"],
            "Primary Address Postal": [pd.NA],
            "Primary Phone Number": [pd.NA],
            "Primary Other Phone": [pd.NA],
            "Primary Mobile Phone": [pd.NA],
            "Primary Work Phone": [pd.NA],
            "Primary Email Address": [pd.NA],
            "Primary Other Email": [pd.NA],
            "Primary Work Email": [pd.NA],
        }
    )

    normalized = normalize_df(
        source,
        config.COLUMNS,
        [field for field, value in config.COLUMNS if value],
    )

    assert pd.isna(normalized["clean_Primary First Name:name"].iloc[0])
    assert pd.isna(normalized["clean_Primary Phone Number:phone"].iloc[0])
    assert normalized["clean_Primary Last Name:name"].iloc[0] == "bird"


def test_bounds_reject_nan_out_of_range_and_reversed_values():
    with pytest.raises(ValueError, match="between 0 and 100"):
        Bounds(u_bound=101, l_bound=50)
    with pytest.raises(ValueError, match="less than or equal"):
        Bounds(u_bound=40, l_bound=50)


def test_config_loader_reports_invalid_bounds(tmp_path):
    invalid_config = tmp_path / "invalid.yaml"
    invalid_config.write_text(CONFIG_PATH.read_text().replace("u_bound: 90", "u_bound: 101"))

    with pytest.raises(ConfigError, match="matching bounds"):
        Utilities.load_client_config(invalid_config)


def test_column_weights_must_be_in_range():
    with pytest.raises(ValueError, match="between 0 and 1"):
        ColumnTypeConfig(columns=["Email"], weight=1.1)


def _weight_config(columns, *, phone=None, email=None, name=None):
    contact_configs = {}
    for key, weight in (("phone", phone), ("email", email), ("name", name)):
        if key not in columns:
            continue
        contact_configs[key] = {"columns": columns[key]}
        if weight is not None:
            contact_configs[key]["weight"] = weight
    return ClientConfig.model_validate(
        {
            "CLIENT_NAME": "weights",
            "COLUMNS": contact_configs,
            "CANDIDATE_BLOCKS": {},
            "BLOCKING": {"strict": False, "type": "name", "column": "Name"},
            "MAIN_MATCH_CRITERIA": next(iter(next(iter(columns.values())))),
            "MATCH_FIELD": next(iter(next(iter(columns.values())))),
            "BOUNDS": {},
        }
    )


def test_contact_type_weight_is_split_across_its_columns():
    config = _weight_config(
        {"phone": ["Home", "Work", "Mobile"], "email": ["Email"]},
        phone=0.25,
        email=0.75,
    )

    assert config.weight_for("phone", "Home") == pytest.approx(1 / 12)
    assert config.weight_for("phone", "Work") == pytest.approx(1 / 12)
    assert config.weight_for("email", "Email") == pytest.approx(0.75)


def test_missing_column_weight_warns_and_can_be_auto_balanced():
    with pytest.warns(UserWarning, match="name.Full Name"):
        config = _weight_config(
            {"name": ["First Name", "Last Name", "Full Name"]},
            name=[["First Name", 0.2], ["Last Name", 0.3]],
        )

    config.auto_balance_weights()

    assert config.weight_for("name", "First Name") == pytest.approx(0.2 / 0.75)
    assert config.weight_for("name", "Last Name") == pytest.approx(0.3 / 0.75)
    assert config.weight_for("name", "Full Name") == pytest.approx(0.25 / 0.75)
    assert sum(weight for _, weight in config.COLUMNS.name.weight) == pytest.approx(1.0)


def test_complete_explicit_weights_must_total_one():
    with pytest.warns(UserWarning, match="not 1.0"):
        config = _weight_config(
            {"name": ["First Name", "Last Name"]},
            name=[["First Name", 0.2], ["Last Name", 0.2]],
        )

    config.auto_balance_weights()

    assert config.weight_for("name", "First Name") == pytest.approx(0.5)
    assert config.weight_for("name", "Last Name") == pytest.approx(0.5)


def test_weights_greater_than_one_are_proportionally_reduced():
    with pytest.warns(UserWarning, match="not 1.0"):
        config = _weight_config(
            {"phone": ["Phone"], "email": ["Email"]},
            phone=0.75,
            email=0.75,
        )

    config.auto_balance_weights()

    assert config.weight_for("phone", "Phone") == pytest.approx(0.5)
    assert config.weight_for("email", "Email") == pytest.approx(0.5)


def test_combine_fields_preserves_missing_values_and_ignores_blanks():
    fields = [
        pd.Series([" 123 ", pd.NA]),
        pd.Series([pd.NA, "Main"]),
        pd.Series(["   ", " Street "]),
    ]

    combined = combine_fields(fields, "address")

    assert combined.tolist() == ["123", "MainStreet"]
    assert combined.name == "address_combined"


def test_add_record_ids_vectorized_fallback_uses_positions_not_index():
    from contact_dedupe.dedupe.normalize import add_record_ids

    source = pd.DataFrame({"value": ["a", "b"]}, index=[10, 42])

    result = add_record_ids(source)

    assert result["_record_id"].tolist() == ["record:0", "record:1"]
    assert result.index.tolist() == [10, 42]
