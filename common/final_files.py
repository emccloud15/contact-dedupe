import pandas as pd
import numpy as np
import re
from datetime import datetime
from pathlib import Path
import sys

from common.models import Virtuous

def clean_column(col: str)-> str | None:
    if col == 'Duplicate dupe':
        return 'Merge'
    
    if col.startswith(('clean_','idx','order','count','root')):
        return None
    if col in ['dupe', 'score', 'match_id']:
        return None
    if any(word in col for word in ['root','count','order']):
        return None
    if ((col.startswith('Duplicate clean_')) & (not col.endswith('dupe'))):
        return None
    
    prefix = "" if col.startswith("Duplicate") else "Duplicate"

    match = re.search(r'clean_(.*?)_dupe', col)
    if match:
        middle = re.sub(r':.*', '', match.group(1))
        middle = middle.replace('_',' ')
        return f"{prefix} {middle}".strip()
    else:
        return col
    

# Columns for the final check file
def create_check_cols(orig_cols: list[str]) -> list:
    return [col for col in orig_cols if not 
            ((col.startswith('clean') or col.startswith('score') or col.startswith('main')) & col.endswith('_main')) |
            (col.startswith('clean') & ('dupe' not in col) ) | 
            (col.startswith('dupe')) | 
            ('combined' in col) |
            ('root' in col) |
            ('count' in col) |
            ('match_id_duplicate' in col) |
            ('match_id_main' in col)]
  
def create_check_file(df: pd.DataFrame, output_path: str, u_bound: float) -> None:

    check_file = df.merge(df, how='inner', left_on='Id', right_on='match_id', suffixes=('_main', '_duplicate'))
    check_file = check_file[check_file['Id_main'] != check_file['Id_duplicate']]
    
    cols = create_check_cols(list(check_file.columns))
    check_file = check_file[cols]
    check_file.insert(0,'Merge','MERGE', allow_duplicates=True)
    mask = check_file['score_duplicate'] < u_bound
    check_file.loc[mask, 'Merge'] = 'CHECK'
    

    check_file.to_csv(output_path, index=False)

def create_virtuous_file(df: pd.DataFrame, output_path: str, u_bound: float, l_bound: float) -> None:
    primary_df = df[df['order'] == 1]
    comparative_df = df[df['order'] == 2]
    

    compared_record_cols = [f"Duplicate {col}" for col in df.columns]
    comparative_df.columns = compared_record_cols

    primary_df = primary_df.set_index('idx')
    comparative_df = comparative_df.set_index('Duplicate idx')
    

    final_df = pd.concat([primary_df,comparative_df], axis=1)
    final_df.to_csv(output_path, index=False)
    sys.exit()
    

    # This labels rows where 1 and only one value matched. If a record only matched on email they should be given a look
    duplicate_cols = [col for col in compared_record_cols if col.endswith('_dupe')]
    conditions = [
        (final_df['Duplicate score'] == 0) & (final_df[duplicate_cols].sum(axis=1) == 1),
        (final_df['Duplicate score'] == 0) & (final_df[duplicate_cols].sum(axis=1) > 1),
    ]
    choices = [
        45,
        55,
    ]

    final_df['Duplicate score'] = np.select(condlist=conditions, choicelist=choices, default=final_df['Duplicate score'])

    conditions = [
        (final_df['Duplicate score'] <= u_bound) & (final_df['Duplicate score'] >= l_bound),
        (final_df['Duplicate score'] < l_bound),
        (final_df['Duplicate score'] > u_bound)
    ]
    choices = [
        'CHECK',
        'IGNORE',
        'MERGE'
    ]

    # Mask ensures if contact type is ignored from earlier that does not get overwritten

    final_df.loc['Merge'] = np.select(condlist=conditions, choicelist=choices, default='CHECK')

    
    col_map = {col : clean_column(col) for col in final_df.columns}
    final_df = final_df[[col for col,new in col_map.items() if new]]
    final_df.columns = [new for new in col_map.values() if new]

    
    final_df.to_csv(output_path, index=False)

def create_final_file(
    df: pd.DataFrame,
    output_path: Path,
    client_name: str,
    u_bound: float,
    l_bound: float,
    virtuous: Virtuous | None
) -> None:
    
    today = datetime.today().date()
    df['score'] = df['score'].round(0)


    # Master file output
    df.sort_values("match_id").to_csv(
        f"{output_path}/{client_name}_master_{today}.csv", index=False
    )

    # Check file output
    # check_output_path = f"{output_path}/{client_name}_check_{today}.csv"
    # create_check_file(df, check_output_path, u_bound)


    # Virtuous file output
    if virtuous:
        virtuous_output_path = f"{output_path}/{client_name}_virtuous_{today}.csv"
        create_virtuous_file(df=df, output_path=virtuous_output_path, u_bound=u_bound, l_bound=l_bound)
        
       