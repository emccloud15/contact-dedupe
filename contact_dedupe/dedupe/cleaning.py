import re
from typing import Optional, cast

import pandas as pd


ScalarValue = str | int | float | None


def _is_missing(value: ScalarValue) -> bool:
    """Return whether a scalar value is null or contains only whitespace."""
    if value is None or cast(bool, pd.isna(value)):
        return True
    return isinstance(value, str) and not value.strip()


def clean_name(n: ScalarValue) -> Optional[str]:
    if _is_missing(n):
        return None
    cleaned = re.sub(r"[^a-zA-Z]", "", str(n)).lower()
    return cleaned or None


def clean_email(e: ScalarValue) -> Optional[str]:
    if _is_missing(e):
        return None
    return str(e).strip().lower().replace(" ", "")


def clean_phone(p: ScalarValue) -> Optional[str]:
    if _is_missing(p):
        return None

    cleaned = re.sub(r"\D", "", str(p))
    if len(cleaned) == 11 and cleaned.startswith("1"):
        cleaned = str(cleaned[1:])
    return cleaned or None


def clean_address(a: ScalarValue) -> Optional[str]:
    if _is_missing(a):
        return None
    
    a = str(a)
    a = a.split("-")[0]
    cleaned = re.sub(r"[\s\"\'\-,\.]", "", str(a)).lower()
    return cleaned or None


def _missing_series(values: pd.Series) -> pd.Series:
    """Return the missing/blank mask shared by the vectorized cleaners."""
    text = values.astype("string")
    return values.isna() | text.str.strip().eq("")


def clean_name_series(values: pd.Series) -> pd.Series:
    """Vectorized equivalent of :func:`clean_name`."""
    text = values.astype("string")
    cleaned = text.str.replace(r"[^a-zA-Z]", "", regex=True).str.lower()
    return cleaned.mask(_missing_series(values) | cleaned.eq(""))


def clean_email_series(values: pd.Series) -> pd.Series:
    """Vectorized equivalent of :func:`clean_email`."""
    text = values.astype("string")
    cleaned = text.str.strip().str.lower().str.replace(" ", "", regex=False)
    return cleaned.mask(_missing_series(values) | cleaned.eq(""))


def clean_phone_series(values: pd.Series) -> pd.Series:
    """Vectorized equivalent of :func:`clean_phone`."""
    text = values.astype("string")
    cleaned = text.str.replace(r"\D", "", regex=True)
    leading_country_code = cleaned.str.len().eq(11) & cleaned.str.startswith("1")
    cleaned = cleaned.mask(leading_country_code, cleaned.str[1:])
    return cleaned.mask(_missing_series(values) | cleaned.eq(""))


def clean_address_series(values: pd.Series) -> pd.Series:
    """Vectorized equivalent of :func:`clean_address`."""
    text = values.astype("string")
    cleaned = (
        text.str.split("-", n=1).str[0]
        .str.replace(r"[\s\"\'\-,\.]", "", regex=True)
        .str.lower()
    )
    return cleaned.mask(_missing_series(values) | cleaned.eq(""))
