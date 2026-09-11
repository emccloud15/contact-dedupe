import pandas as pd
import numpy as np
import re
from datetime import datetime
from pathlib import Path
import sys
from typing import Optional



    

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




       