import pandas as pd
from typing import Callable
import sys

from dedupe.cleaning import clean_name, clean_email, clean_phone, clean_address

from common.models import Columns
from common.exceptions import ConfigError

# For fuzzy later. If there could be nicknames causing duplicates. "Christina" "Tina". This takes every name from the provided name column (typically the firstname column)
# Does a lookup in the python nicknames package dictionary and replaces nicknames with their name. If "Tina" is in the name column it will be replaced with "Christina"


# This function is used when calling the cleaning function incase a column name in the yaml is not actually in the dataframe
def safe_apply(df: pd.DataFrame, col: str, clean_fn: Callable[[str], str | None]) -> pd.Series:
    try:
        return df[col].apply(clean_fn)
    except KeyError:
        raise ConfigError(f"Column name is not in the dataframe: {col}")
    except Exception as e:
        raise ConfigError(f"Error processing column: {col}: {e}")

def combine_address(addresses: list[pd.Series]) -> pd.Series:
    address_df = pd.concat(addresses, axis=1)
    def join_row(row: pd.Series) ->str:
        vals = [v for v in row if pd.notna(v)]
        return "|".join(vals) if vals else pd.NA # type: ignore
    return address_df.apply(join_row, axis=1).rename("address")

# Step 3. Create the normalized columns provided by cleaning them and combining them into one column
# Only creates rows when there is no null value for one of the fields being combined.
# For example if name is John Smith and phone is null instead of getting johnsmith|
# the row would be null. If name is John Smith and phone is (123)-7645555 the result is johnsmith|1237645555
def normalize_contact_method(
    df: pd.DataFrame,
    data: Columns,
    contact_type: str,
    name_cache: dict
) -> pd.DataFrame:
    
    df = df.copy()
    clean_fns = {
        'name': clean_name,
        'address': clean_address,
        'phone': clean_phone,
        'email': clean_email
    }
    clean_fn = clean_fns[contact_type]

    columns = [col for col in getattr(data, contact_type).columns]

    contact_type_series = [safe_apply(df=df, col=col, clean_fn=clean_fn) for col in columns]

    if contact_type == 'name':
        name_cache['names'] = contact_type_series
  
    if contact_type == 'address':
        contact_type_series = [combine_address(contact_type_series)]
    
    for series in contact_type_series:
        

        mask = series.notna()
        parts = [series[mask]]
       

        if contact_type != 'name' and getattr(data, contact_type).include_name:

            parts += [s[mask] for s in name_cache['names']]
            joined_name = pd.concat(parts, axis=1).agg("|".join, axis=1)

            col = f'clean_{series.name}:name_{contact_type}'
            
            df[col] = pd.NA
            df.loc[mask, col] = joined_name
        
        col = f'clean_{series.name}:{contact_type}'
        df[col] = pd.NA
        df.loc[mask, col] = series

    return df[[c for c in df.columns if c.startswith("clean_")]]



# Step 1. Uncleaned Original Data Frame passed as well as the columns from the client yaml as data
def normalize_df(df: pd.DataFrame, data: Columns) -> pd.DataFrame:
    
    # Ensures the name data is passed into the normalizing function first. That way i can clean the name data and store it
    # to be used in case we want to append the names to the contact columns

    contact_type_order = ['name','email','phone','address']
    contact_types = [field for field,value in data if value]
    contact_types.sort(key=lambda x: contact_type_order.index(x))

    name_cache ={}
    final_cleaned_dfs = [
        normalize_contact_method(df=df, data=data, contact_type=contact_type, name_cache=name_cache)
        for contact_type in contact_types]
    
    return df.join(final_cleaned_dfs) # type: ignore
