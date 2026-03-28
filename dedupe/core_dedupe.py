import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz
from typing import Optional
import sys

from common.logger import get_logger
logger = get_logger(__name__)


def run_strict_dedupe(normalized_df: pd.DataFrame, cols: list[str], dsu: object):
    normalized_df.loc[:,'dupe'] = pd.NA
    # Dedupe on each of the client chosen normalized columns
    for col in cols:
        #duped_df = normalized_df[normalized_df['dupe'] != 'TRUE']
        # Use DSU to link multiple dupes to one root record
        for idx_group in normalized_df.groupby(col).indices.values():
            if len(idx_group) > 1:
                for i in range(1, len(idx_group)):
                    dsu.union(idx_group[0],idx_group[i])
                    
        
        # Label duplicated records for master df 
        mask = normalized_df[col].duplicated(keep=False) & normalized_df[col].notna()
        normalized_df.loc[mask,f"{col}_dupe"] = 'TRUE'
        normalized_df.loc[~mask,f"{col}_dupe"] = 'FALSE'
        normalized_df.loc[mask,'dupe'] = 'TRUE'
    
        normalized_df.loc[mask,'count'] = (normalized_df[mask].groupby(col).cumcount() + 1)

    unassaigned_mask = normalized_df['count'].isna()
    normalized_df.loc[unassaigned_mask,'count'] = 1
    
    return normalized_df

def label_df(main_df: pd.DataFrame, l_bound: Optional[float] = 80.0, u_bound: Optional[float] = 86.0):
    mask = main_df['dupe'] == 'TRUE'
    main_df.loc[mask,'score'] = 100.0
    mask = main_df['score'].isna()
    main_df.loc[mask,'dupe'] = 'FALSE'
    mask = (main_df['score'] >= l_bound) & (main_df['score']<=u_bound)
    main_df.loc[mask, 'dupe'] = 'CHECK'
    mask = main_df['dupe'].isna()
    main_df.loc[mask,'dupe'] = 'TRUE'

def assign_scores(final_matrix: np.ndarray, block_df: pd.DataFrame, score_array: np.array, dsu: object, u_bound: float, l_bound: float):
    
    upper = np.triu(final_matrix,k=1)
    pairs = np.argwhere(upper >= l_bound)
    if len(pairs) <1:
        return
    
    for i,j in pairs:
        dsu.union(block_df.index[i],block_df.index[j])


    rows,columns = pairs[:,0],pairs[:,1]
    scores = upper[rows,columns]

    np.maximum.at(score_array, block_df.index[rows], scores)
    np.maximum.at(score_array, block_df.index[columns], scores)
    


def run_fuzzy_dedupe(main_df: pd.DataFrame, cols: dict, dsu: object, blocking: str, bounds: list[dict]):
    
    score_array = np.zeros(len(main_df))

    u_bound = bounds[1]['u_bound']
    l_bound = bounds[0]['l_bound']

    # Creating blocks to fuzzy on. Can be changed in clieny yaml
    dupe_df = main_df[main_df['count']==1]
    for _,block_df in dupe_df.groupby(dupe_df[blocking].str[:3]):
        n = len(block_df)
        if n < 2:
            continue

        
        final_matrix = np.zeros((n,n))
        matrices = {f"{col.split(':')[1].strip()}" if ':' in col else col: np.zeros((n,n)) for col in cols.keys()} 
        
        # Normalized columns to fuzzy on
        for col,weight in cols.items():
            records = block_df[col].to_list()
            scores = process.cdist(records,records, scorer=fuzz.WRatio) 
            final_matrix += (scores * weight)
            matrices[f"{col.split(':')[1].strip() if ':' in col else col}"] += scores
            if '5S-EWTN-254538' in block_df['legacycontactid'].values:
                print(col)
                print(scores[47,3])
                print(final_matrix[47,3])

        
        
        hits = {k:v >= 95 for k,v in matrices.items()}
        hit_count = sum(v for v in hits.values())
    
        gate_mask = (matrices['address'] >= 95.0) & (hit_count >= 2)
        final_matrix += (hit_count/2)
        
        
        final_matrix = np.where(gate_mask, final_matrix, 0)
        if ('5S-EWTN-254538' in block_df['legacycontactid'].values):
            print(final_matrix[47,3])
       
                
        # Assign scores to df
        assign_scores(final_matrix, block_df, score_array, dsu, u_bound,l_bound)


    
    
    mask = score_array > 0
    main_df.loc[mask,'score'] = score_array[mask]
    
    label_df(main_df,l_bound=l_bound, u_bound=u_bound)

    return main_df
            

                

            
                    
                