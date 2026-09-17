import pandas as pd
from contact_dedupe.dedupe.cleaning import clean_name


def test_clean_name_keeps_missing_values_missing():
    assert clean_name(pd.NA) is None
