from pathlib import Path
import pandas as pd
import numpy as np
from common.utils import load_client_config, load_data_df

from typing import Tuple

TEST_DIR = Path(__file__).parent
yaml_path_virtuous = TEST_DIR / "data/test.yaml"
df_path_virtuous = TEST_DIR / "data/test.csv"





def test_return_value_for_virtuous_option():
    settings = load_client_config(yaml_path_virtuous)
    assert settings.VIRTUOUS
    

def test_virtuous_table_setup_contact_type_true():
    df=load_data_df(df_path_virtuous)
    contact_type = True

    try:
        # The virtuous output file names all the duplicate fields starting with 'Duplicate' except for the legacy id field. They name that 'Legacy Duplicate Id' 
        df = df.rename(columns={'Legacy Duplicate Id': 'Duplicate Legacy Id'})
    except:
        pass
    df.loc[:,'Merge'] = "CHECK"
    if contact_type:
        mask = (df['Type'] != df['Duplicate Type'])
        df.loc[mask,'Merge'] = "IGNORE"

    df.loc[:,'idx'] = df.index
    df.loc[:,'Duplicate idx'] = df.index
    df.loc[:,'order'] = 1
    df.loc[:,'Duplicate order'] = 2
    df.loc[:, 'Duplicate Match Qualifiers'] = df.loc[:, 'Match Qualifiers']
    primary_cols = [col for col in df.columns if 'Duplicate' not in col]
    duplicate_cols = ['Duplicate ' + col for col in primary_cols if 'Merge' not in col] + ['Merge']
    primary_df = df.loc[:,primary_cols]
    duplicate_df = df.loc[:,duplicate_cols]

    duplicate_df.columns = primary_df.columns

    main_df = pd.concat([primary_df, duplicate_df], ignore_index=True)
    mask = main_df['Merge'] == "CHECK"
    main_df = main_df.loc[mask]
    virtuous_contact_type_df = main_df.loc[~mask]

    assert all(main_df['Merge']!="IGNORE")
    assert all(main_df['Merge']=='CHECK')
    assert all(virtuous_contact_type_df['Merge']!="CHECK")
    assert all(virtuous_contact_type_df['Merge']=="IGNORE")
    

def test_virtuous_table_setup_contact_type_false():
    df=load_data_df(df_path_virtuous)
    contact_type = False

    try:
        # The virtuous output file names all the duplicate fields starting with 'Duplicate' except for the legacy id field. They name that 'Legacy Duplicate Id' 
        df = df.rename(columns={'Legacy Duplicate Id': 'Duplicate Legacy Id'})
    except:
        pass
    df.loc[:,'Merge'] = "CHECK"
    if contact_type:
        mask = (df['Type'] != df['Duplicate Type'])
        df.loc[mask,'Merge'] = "IGNORE"

    df.loc[:,'idx'] = df.index
    df.loc[:,'Duplicate idx'] = df.index
    df.loc[:,'order'] = 1
    df.loc[:,'Duplicate order'] = 2
    df.loc[:, 'Duplicate Match Qualifiers'] = df.loc[:, 'Match Qualifiers']
    primary_cols = [col for col in df.columns if 'Duplicate' not in col]
    duplicate_cols = ['Duplicate ' + col for col in primary_cols if 'Merge' not in col] + ['Merge']
    primary_df = df.loc[:,primary_cols]
    duplicate_df = df.loc[:,duplicate_cols]

    duplicate_df.columns = primary_df.columns

    main_df = pd.concat([primary_df, duplicate_df], ignore_index=True)
    mask = main_df['Merge'] == "CHECK"
    main_df = main_df.loc[mask]
    virtuous_contact_type_df = main_df.loc[~mask]

    assert all(main_df['Merge']=='CHECK')
    assert virtuous_contact_type_df.empty