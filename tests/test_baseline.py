"""Phase 1 regression coverage for the current public input and cleaning API."""

from pathlib import Path

import pandas as pd

from contact_dedupe.common.models import ClientConfig
from contact_dedupe.common.utils import Utilities
from contact_dedupe.dedupe.cleaning import (
    clean_address,
    clean_email,
    clean_phone,
    clean_name,
)
from contact_dedupe.dedupe.normalize import normalize_df


TEST_DIR = Path(__file__).parent
CONFIG_PATH = TEST_DIR / "data" / "test.yaml"
CSV_PATH = TEST_DIR / "data" / "test.csv"


def test_config_loader_returns_client_config():
    config = Utilities.load_client_config(CONFIG_PATH)

    assert isinstance(config, ClientConfig)
    assert config.CLIENT_NAME == "MARY_BIRD"
    assert config.COLUMNS.name.columns == ["Primary First Name", "Primary Last Name"]


def test_csv_loader_preserves_configured_source_columns():
    df = Utilities.load_data_df(CSV_PATH)

    assert len(df) > 0
    assert {"Id", "Primary Email Address", "Primary Address Postal"}.issubset(df.columns)


def test_cleaning_functions_normalize_representative_values():
    assert clean_name("Mary-Jane O'Neil") == "maryjaneoneil"
    assert clean_email("  Mary.Bird @Example.COM ") == "mary.bird@example.com"
    assert clean_phone("+1 (225) 555-0100") == "2255550100"
    assert clean_address("  123 Main St. - Apt 4  ") == "123mainst"


def test_normalize_df_retains_original_values_and_adds_clean_fields():
    config = Utilities.load_client_config(CONFIG_PATH)
    source = pd.DataFrame(
        {
            "Id": [101, 102],
            "Primary First Name": ["Mary-Jane", "Mary"],
            "Primary Last Name": ["O'Neil", "Bird"],
            "Primary Address Line 1": ["123 Main St.", ""],
            "Primary Address Line 2": [pd.NA, pd.NA],
            "Primary Address City": ["Baton Rouge", "Baton Rouge"],
            "Primary Address State": ["LA", "LA"],
            "Primary Address Postal": ["70801-1234", "70801"],
            "Primary Phone Number": ["+1 (225) 555-0100", pd.NA],
            "Primary Other Phone": [pd.NA, pd.NA],
            "Primary Mobile Phone": [pd.NA, pd.NA],
            "Primary Work Phone": [pd.NA, pd.NA],
            "Primary Email Address": ["mary@example.com", pd.NA],
            "Primary Other Email": [pd.NA, pd.NA],
            "Primary Work Email": [pd.NA, pd.NA],
        }
    )

    normalized = normalize_df(
        df=source,
        data=config.COLUMNS,
        contact_types=[field for field, value in config.COLUMNS if value],
    )

    assert normalized["Primary First Name"].tolist() == ["Mary-Jane", "Mary"]
    assert normalized["clean_Primary First Name:name"].tolist() == ["maryjane", "mary"]
    assert normalized["clean_Primary Phone Number:phone"].iloc[0] == "2255550100"
    assert normalized["clean_Primary Email Address:email"].iloc[0] == "mary@example.com"
    assert normalized["clean_Primary Address Line 1:address"].iloc[0] == "123mainst"
    assert normalized["clean_Primary Address Postal:address"].iloc[0] == "70801"
