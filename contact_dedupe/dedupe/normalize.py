import pandas as pd
from typing import Callable
import click

from .cleaning import clean_name, clean_email, clean_phone, clean_address

from contact_dedupe.common.models import Columns
from contact_dedupe.common.exceptions import ConfigError



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
        return ''.join(vals) if vals else pd.NA # type: ignore
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
        'address': clean_address,
        'phone': clean_phone,
        'email': clean_email
    }
    clean_fn = clean_fns[contact_type]

    # Get columns for contact type from client config yaml
    columns = [col for col in getattr(data, contact_type).columns]

    # Safely apply cleaning functions to each contact type col and build a list of series. 
    # If there are 3 phone fields this will be a list of each phone field cleaned in a series.
    # Cleaning functions can be found in cleaning file. 
    contact_type_series = [safe_apply(df=df, col=col, clean_fn=clean_fn) for col in columns]

    
    # Because addresses will always hve multiple columns but should always be compared together all address columns will be combined. 
    if contact_type == 'address':
        contact_type_series = [combine_address(contact_type_series)]
    

    for series in contact_type_series:
        
        mask = series.notna()
        parts = [series[mask]]
        
        # Adding the name columns from the name cache if user set include name to true in yaml.
        if getattr(data, contact_type).include_name:

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
def normalize_df(df: pd.DataFrame, data: Columns, contact_types: list[str]) -> pd.DataFrame:
    
    # Build a cache of cleaned name columns to be attached to other contact cols if user choice
    name_cache ={}
    if data.name:
        name_cols = [value for value in data.name.columns]
        name_cache['names'] = [safe_apply(df,col,clean_name) for col in name_cols]


    with click.progressbar(contact_types, label='cleaning data') as bar:
        # Passing every contact type and their respective yaml column data except name.
        final_cleaned_dfs = [
            normalize_contact_method(df=df, data=data, contact_type=ct, name_cache=name_cache)
            for ct in bar if ct != 'name']
    
    return df.join(final_cleaned_dfs) # type: ignore
