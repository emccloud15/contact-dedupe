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
