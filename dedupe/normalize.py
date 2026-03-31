import pandas as pd
import sys
from typing import Callable, Optional

from common.exceptions import ConfigError

from dedupe.cleaning import clean_name, clean_email, clean_phone, clean_address



# For fuzzy later. If there could be nicknames causing duplicates. "Christina" "Tina". This takes every name from the provided name column (typically the firstname column)
# Does a lookup in the python nicknames package dictionary and replaces nicknames with their name. If "Tina" is in the name column it will be replaced with "Christina"



# This function is used when calling the cleaning function incase a column name in the yaml is not actually in the dataframe
def safe_apply(df: pd.DataFrame, col: str, clean_fn: Callable[[str],str]):
    try:
        return df[col].apply(clean_fn)
    except KeyError:
        raise ConfigError(f"Column name is not in the dataframe: {col}")
    except Exception as e:
        raise ConfigError(f"Error processing column: {col}: {e}")


# Step 3. Create the normalized columns provided by cleaning them and combining them into one column
# Only creates rows when there is no null value for one of the fields being combined. 
# For example if name is John Smith and phone is null instead of getting johnsmith|
# the row would be null. If name is John Smith and phone is (123)-7645555 the result is johnsmith|1237645555
def normalize_contact_method(df: pd.DataFrame, data: object, contact_type: str, contact_cols: list[str], name_cols: Optional[list[str]] = None) -> pd.DataFrame:

    df = df.copy()
    if name_cols:
        names =[safe_apply(df,name, clean_name) for name in name_cols]
    

    if contact_type == 'address':

        address_series = [safe_apply(df,address,clean_address) for address in contact_cols]

        address_df = pd.concat(address_series, axis=1)
        
        # Ensuring no composite keys are created with na values 
        mask = address_df.notna().any(axis=1)
        joined_address =(
            address_df[mask].apply(lambda row: "|".join(row.dropna().astype(str)), axis=1)
        )
        parts = [joined_address.astype(str)]

        if data.include_name:
            parts +=[s[mask].astype(str) for s in names]
            joined_name = (
                pd.concat(parts, axis=1).loc[mask].agg("|".join, axis=1)
            )

            df["clean_address:name_address"] = pd.NA
            df.loc[mask, f"clean_address:name_address"] = joined_name

        df["clean_address:address"] = pd.NA
        df.loc[mask,"clean_address:address"] = joined_address
        

        
        
        return df[[c for c in df.columns if c.startswith("clean_")]]

    else:
        if contact_type == 'phone':
            func = clean_phone
        else:
            func = clean_email
            
        for contact_col in contact_cols:
            
            contact_cleaned = safe_apply(df,contact_col,func)
            
            mask = contact_cleaned.notna()

            parts = [contact_cleaned[mask].astype(str)]

            if data.include_name:
                parts += [s[mask].astype(str) for s in names]
                joined_name = (
                pd.concat(parts, axis=1).loc[mask].agg("|".join, axis=1)
            )
                df[f"clean_{contact_col}:name_{contact_type}"] = pd.NA
                df.loc[mask, f"clean_{contact_col}:name_{contact_type}"] = joined_name
        
            joined = (
                pd.concat(parts, axis=1).loc[mask].agg("|".join, axis=1)
            )

            df[f"clean_{contact_col}:{contact_type}"] = pd.NA
            df.loc[mask, f"clean_{contact_col}:{contact_type}"] = joined
       
    return df[[c for c in df.columns if c.startswith("clean_")]]

def normalize_helper(df: pd.DataFrame, data: object, contact_type: str, name_cols: Optional[list[str]] = None):
    data = getattr(data, contact_type)
    if not data.include_name:
        name_cols = None
    return normalize_contact_method(df=df, data=data, contact_type=contact_type, contact_cols=data.columns, name_cols=name_cols)



# Step 1. Uncleaned Original Data Frame passed as well as the columns from the client yaml as data
def normalize_df(df: pd.DataFrame, data: object) -> pd.DataFrame:   
    # Checks which name columns to include in normalized columns
    if data.name:
        name_cols = [col for col in data.name.columns]
    
    
    contact_types = [field for field,value in data if value is not None]

    final_cleaned_dfs = [normalize_helper(df,data,contact_type,name_cols) for contact_type in contact_types if contact_type != 'name']
    return df.join(final_cleaned_dfs)
