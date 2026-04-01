import pandas as pd
import re

def clean_name(n):
    return re.sub(r"[^a-zA-Z]",'', str(n)).lower()

def clean_email(e):
    if pd.isna(e):
        return pd.NA
    e = str(e)
    return e.strip().lower().replace(' ','')

def clean_phone(p):
    if pd.isna(p):
        return pd.NA
    cleaned = re.sub(r"\D",'',str(p))
    if len(cleaned) == 11 and cleaned.startswith('1'):
        cleaned = str(cleaned[1:])
    return cleaned

def clean_address(a):
    if pd.isna(a):
        return pd.NA
    a = str(a)
    return re.sub(r'[\s\"\'\-,]','',str(a)).lower()