import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
import numpy as np
from rapidfuzz import process, fuzz, distance
from typing import Optional
from itertools import combinations
from nicknames import NickNamer

from dedupe.dsu import DSU
from common.exceptions import ConfigError
from common.logger import get_logger
logger = get_logger(__name__)


def run_strict_dedupe(normalized_df: pd.DataFrame, cols: list[str], dsu: DSU, blocks: DataFrameGroupBy):
    normalized_df.loc[:,'dupe'] = pd.NA
    normalized_df.loc[:,'score'] = pd.NA
 
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
        normalized_df.loc[mask, 'dupe'] = 'TRUE'
        normalized_df.loc[~mask, 'dupe'] = 'FALSE'
    
        normalized_df.loc[mask,'count'] = (normalized_df[mask].groupby(col).cumcount() + 1)

    unassaigned_mask = normalized_df['count'].isna()
    normalized_df.loc[unassaigned_mask,'count'] = 1
    
    return normalized_df

def label_df(main_df: pd.DataFrame, l_bound: float, u_bound: float):
    mask = main_df['dupe'] == 'TRUE'
    main_df.loc[mask, 'score'] = 100

    mask = main_df['score'] > u_bound
    main_df.loc[mask,'dupe'] = 'TRUE'

    mask = (main_df['score'] >= l_bound) & (main_df['score']<=u_bound)
    main_df.loc[mask, 'dupe'] = 'CHECK'

    mask = (main_df['score'] < l_bound) | (main_df['score'].isna())
    main_df.loc[mask,'dupe'] = 'FALSE'



def assign_scores(final_matrix: np.ndarray, block_df: pd.DataFrame, score_array: np.array, dsu: DSU, l_bound: float):
    
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






def run_fuzzy_dedupe(main_df: pd.DataFrame, cols: dict, dsu: DSU, blocks: DataFrameGroupBy, main_match_criteria: str, u_bound: Optional[float] = 95.0, l_bound: Optional[float] = 80.0, nickname_col: Optional[str] = None):
    
    score_array = np.zeros(len(main_df))

    
    if nickname_col:
        nn = NickNamer()


    # Looping through blocks to fuzzy on. Blocking can be changed in client yaml

    for _,block_df in blocks:
        n = len(block_df)
        if n < 2:
            continue

        final_matrix = np.zeros((n,n))

        # Nickname set for finding name matches between records like "Christina" and "Tina"
        if nickname_col:
            nicknames =  {name : set(nn.nicknames_of(name)) | {name.lower()} for name in block_df[nickname_col].unique()}
            
        # Creating a dictionary of matrices. One for each column being fuzzied on. The columns being fuzzied on are normalized columns from the normalize function
        # They are all in the form "clean_originalcolname:contacttype" i.e clean_homephone:phone, except for address which is just clean_address:address,
        # And name columns which are their original column name
        matrices = {f"{col.split("_")[1].split(':')[0].strip()}" if '_' in col else col: np.zeros((n,n)) for col in cols.keys()} 
        
        # Normalized columns to fuzzy on
        for col,weight in cols.items():
            

            records = block_df[col].to_list()

            # If nickname column has been passed, seperate fuzzying will be performed on this column
            if col == nickname_col:
                

                for (i, name_a), (j, name_b) in combinations(enumerate(records), 2):
                    # Doing a lookup on the nickname dictionary to create an intersecting set for names that are eachothers nicknames
                    match = bool(nicknames.get(name_a, set()) & nicknames.get(name_b, set()))
                    
                    # Giving a score of 100 for names that are eachothers nicknames
                    # The main match criteria used later on will mitigate false positives
                    if match:        
                        matrices[nickname_col][i, j] = 100
                        final_matrix[i, j] += (100 * weight)
                        
                    # No nickname matches, Jaro Winkler will be used to fuzzy match
                    else:
                        score = distance.JaroWinkler.normalized_similarity(name_a, name_b) * 100
                        matrices[nickname_col][i, j] = score
                        final_matrix[i, j] += (score * weight)
            
            # Non nickname columns will use the W Ratio for fuzzying
            else:
                scores = process.cdist(records,records, scorer=fuzz.WRatio) 
                final_matrix += (scores * weight)

                matrices[f"{col.split("_")[1].split(':')[0].strip()}" if '_' in col else col] += scores

        
        # Creates a matrix with how many fields return a matching score of over 95.
        # Creating a count for how many fields the two comparing records have in common
        hits = {k:v >= u_bound for k,v in matrices.items()}
        hit_count = sum(v.astype(int) for v in hits.values())

        
        gate_mask = (matrices[main_match_criteria] >= u_bound) & (hit_count >= 2)


        final_matrix = np.where(gate_mask, final_matrix, 0)
            

        # Assign scores to df
        assign_scores(final_matrix, block_df, score_array, dsu, l_bound)


    mask = score_array > 0
    main_df.loc[mask,'score'] = score_array[mask]
    

    label_df(main_df,l_bound=l_bound, u_bound=u_bound)
   
    return main_df
            

                

            
                    
                