from pathlib import Path

import pandas as pd
import pytest

from contact_dedupe.common.exceptions import ConfigError
from contact_dedupe.common.models import Bounds, ColumnTypeConfig
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
