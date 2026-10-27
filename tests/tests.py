from pathlib import Path
from contact_dedupe.common.utils import Utilities

TEST_DIR = Path(__file__).parent
yaml_path_virtuous = TEST_DIR / "data/test.yaml"
df_path_virtuous = TEST_DIR / "data/test.csv"





def test_client_config_loads_with_current_schema():
    settings = Utilities.load_client_config(yaml_path_virtuous)

    assert settings.CLIENT_NAME == "MARY_BIRD"
    assert settings.MAIN_MATCH_CRITERIA == "address"


def test_csv_loader_returns_dataframe():
    df = Utilities.load_data_df(df_path_virtuous)

    assert not df.empty
    assert "Id" in df.columns
