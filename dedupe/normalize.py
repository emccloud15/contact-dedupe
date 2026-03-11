import pandas as pd
from pydantic import BaseModel
from typing import Callable, Optional

from common.exceptions import ConfigError

from dedupe.cleaning import clean_name, clean_email, clean_phone, clean_address



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
def normalize_contact_method(df: pd.DataFrame, contact_cols: list[str], contact_type: str, name_cols: Optional[list[str]]= None) -> pd.DataFrame:

    df = df.copy()

    if name_cols:
        names =[safe_apply(df,name, clean_name) for name in name_cols]
        name_mark = 'name'
    else:
        name_mark=''
        names = []

    

    if contact_type == 'address':

        address_series = [safe_apply(df,address,clean_address) for address in contact_cols]

        address_df = pd.concat(address_series, axis=1)
        
        # Ensuring no composite keys are created with na values 
        mask = address_df.notna().any(axis=1)
        joined_address =(
            address_df[mask].apply(lambda row: "|".join(row.dropna().astype(str)), axis=1)
        )
        parts = [joined_address.astype(str)]
        parts +=[s[mask].astype(str) for s in names]
        joined = (
                pd.concat(parts, axis=1).loc[mask].agg("|".join, axis=1)
            )

        df[f"clean_address_{name_mark}:address"] = pd.NA
        df.loc[mask, f"clean_address_{name_mark}:address"] = joined
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
            parts += [s[mask].astype(str) for s in names]
        
            joined = (
                pd.concat(parts, axis=1).loc[mask].agg("|".join, axis=1)
            )


            df[f"clean_{contact_col}_{name_mark}:{contact_type}"] = pd.NA
            df.loc[mask, f"clean_{contact_col}_{name_mark}:{contact_type}"] = joined
       
    return df[[c for c in df.columns if c.startswith("clean_")]]


# Determine which columns want to be included at all and if name included in ck
def column_choice_helper(data: BaseModel) -> tuple[list[str], list[str]]:
    contact_cols_no_name=[]
    contact_cols_name=[]
    for col,info in data.items():
        if not info.include_col:
            continue
        if not info.include_name:
            contact_cols_no_name.append(col)
        else:
            contact_cols_name.append(col)

    return contact_cols_name, contact_cols_no_name



# Step 2. Determine which fields to include in combined columns and whether or not to attach a name field to the key  
def normalize_handler(df: pd.DataFrame, data: BaseModel, name_cols: list[str], contact_type: str) -> list[pd.Series]:
    cleaned_series=[]

    # Df field names for which fields to include name or not
    contact_cols_name, contact_cols_no_name = column_choice_helper(data)
    
    if contact_cols_name:
        df_cleaned_name = normalize_contact_method(df, name_cols=name_cols, contact_cols=contact_cols_name, contact_type=contact_type)
        
        cleaned_series.append(df_cleaned_name)
    
    if contact_cols_no_name:
        df_cleaned_no_name = normalize_contact_method(df, contact_cols=contact_cols_no_name, contact_type=contact_type)
        cleaned_series.append(df_cleaned_no_name)
    
    return cleaned_series
    

# Step 1. Uncleaned Original Data Frame passed as well as the columns from the client yaml as data
def normalize_df(df: pd.DataFrame, data: BaseModel) -> pd.DataFrame:
    final_cleaned_series=[]

    # Checks which name columns to include in composite key
    if data.name:
        name_cols = [col for col,include in data.name.items() if include]
    if data.phone:
        final_cleaned_series += normalize_handler(df,data.phone,name_cols,'phone')
    if data.email:
        final_cleaned_series += normalize_handler(df,data.email,name_cols,'email')
    if data.address:
        final_cleaned_series += normalize_handler(df,data.address,name_cols,'address')
    
    
    return df.join(final_cleaned_series)
