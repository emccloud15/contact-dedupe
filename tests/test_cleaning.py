import pandas as pd
from contact_dedupe.dedupe.cleaning import (
    clean_address_series,
    clean_email_series,
    clean_name,
    clean_name_series,
    clean_phone_series,
)


def test_clean_name_keeps_missing_values_missing():
    assert clean_name(pd.NA) is None


def test_vectorized_cleaners_match_scalar_cleaning_behavior():
    values = pd.Series([" Mary-Jane ", pd.NA, "   ", "123-_", ""])

    cleaned_name = clean_name_series(values)
    assert cleaned_name.iloc[0] == "maryjane"
    assert cleaned_name.iloc[1:].isna().all()

    cleaned_email = clean_email_series(pd.Series([" A @EXAMPLE.COM ", pd.NA]))
    assert cleaned_email.iloc[0] == "a@example.com"
    assert cleaned_email.iloc[1:].isna().all()

    cleaned_phone = clean_phone_series(
        pd.Series(["+1 (225) 555-0100", "not a phone", pd.NA])
    )
    assert cleaned_phone.iloc[0] == "2255550100"
    assert cleaned_phone.iloc[1:].isna().all()

    cleaned_address = clean_address_series(
        pd.Series([" 123 Main St. - Apt 4 ", "---", pd.NA])
    )
    assert cleaned_address.iloc[0] == "123mainst"
    assert cleaned_address.iloc[1:].isna().all()
