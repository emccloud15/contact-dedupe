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
    return re.sub(r"\D",'',str(p))

def clean_address(a):
    if pd.isna(a):
        return pd.NA
    a = str(a)
    return re.sub(r'[\s\"\'\-,]','',str(a)).lower()