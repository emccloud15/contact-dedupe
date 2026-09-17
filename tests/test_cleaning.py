import pandas as pd
import pytest

from contact_dedupe.dedupe.cleaning import clean_name


@pytest.mark.xfail(
    reason="Phase 2: cleaning must preserve missing values instead of converting them to text",
    strict=True,
)
def test_clean_name_keeps_missing_values_missing():
    assert clean_name(pd.NA) is None
