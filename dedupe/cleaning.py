import pandas as pd
import re

from typing import Optional


def clean_name(n: str) -> str:
    return re.sub(r"[^a-zA-Z]", "", str(n)).lower()


def clean_email(e: str) -> Optional[str]:
    if pd.isna(e):
        return None
    e = str(e)
    return e.strip().lower().replace(" ", "")


def clean_phone(p: str | int | float) -> Optional[str]:
    if pd.isna(p):
        return None
    cleaned = re.sub(r"\D", "", str(p))
    if len(cleaned) == 11 and cleaned.startswith("1"):
        cleaned = str(cleaned[1:])
    return cleaned


def clean_address(a: str) -> Optional[str]:
    if pd.isna(a):
        return None
    a = str(a)
    return re.sub(r"[\s\"\'\-,]", "", str(a)).lower()
