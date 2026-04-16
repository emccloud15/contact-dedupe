import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
import numpy as np
from numpy.typing import NDArray
from rapidfuzz import process, fuzz
from typing import Optional
from itertools import combinations
from nicknames import NickNamer
import sys
from dedupe.dsu import DSU

from common.logger import get_logger
from common.exceptions import ConfigError

logger = get_logger(__name__)

    
def label_df(main_df: pd.DataFrame, score_array: NDArray) -> None:
    conditions =[
        main_df['dupe'] == True,
        score_array > 0
    ]
    choices = [
        100,
        score_array
    ]
    main_df['score'] = np.select(condlist=conditions, choicelist=choices, default=0)
    main_df.loc[main_df['score']==0, 'dupe'] = False

    mask = score_array > 0
    main_df.loc[mask, "score"] = score_array[mask]
    mask = main_df['dupe'] == True
    main_df.loc[mask,'score'] = 100
    mask = main_df['score'].isna()
    main_df.loc[mask,'dupe'] = False

def assign_match_id(main_df: pd.DataFrame, dsu: DSU, match_field: str) -> pd.DataFrame:
    main_df['root'] = main_df.index.map(dsu.find)
    try:
        main_df["match_id"] = main_df["root"].map(main_df[match_field])
    except KeyError:
        raise ConfigError(
            f"{match_field} not found in csv file columns. Check MATCH_FIELD assignment in the client yaml."
        )
    return main_df

def assign_scores(
    final_matrix: np.ndarray,
    block_df: pd.DataFrame,
    score_array: NDArray,
    dsu: DSU,
    l_bound: float,
) -> None:

    upper = np.triu(final_matrix, k=1)
    pairs = np.argwhere(upper >= l_bound)
    if len(pairs) < 1:
        return

    for i, j in pairs:
        dsu.union(block_df.index[i], block_df.index[j])

    rows, columns = pairs[:, 0], pairs[:, 1]
    scores = upper[rows, columns]

    np.maximum.at(score_array, block_df.index[rows], scores)
    np.maximum.at(score_array, block_df.index[columns], scores)


def run_strict_dedupe(df: pd.DataFrame, cols: list[str], dsu: DSU, main_match: str) -> pd.DataFrame:
    df.loc[:, "dupe"] = pd.NA
    df.loc[:, "score"] = pd.NA

    # Dedupe on each of the client chosen normalized columns
    for col in cols:
        mask = df[col].duplicated(keep=False) & df[col].notna()
        df.loc[mask, f"{col}_dupe"] = True
        df.loc[~mask, f"{col}_dupe"] = False
    
    dupe_cols = [f"{col}_dupe" for col in cols]
    mask = (df[dupe_cols] == True).sum(axis=1) >=3
    df.loc[mask, 'dupe'] = True
    df.loc[mask,"combined_cols"] = df.apply(lambda row: "|".join([row[col] for col in cols if row[f"{col}_dupe"] == True]), axis=1)
    
    # DSU to link multiple dupes to one root record
    for idx_group in df.groupby("combined_cols").indices.values():
            if len(idx_group) > 1:
                label_indices = df.index[idx_group]
                for i in range(1, len(label_indices)):
                    dsu.union(label_indices[0], label_indices[i])
    
    df = assign_match_id(main_df=df,dsu=dsu, match_field=main_match)
    df["count"] = df.groupby("match_id").cumcount() + 1
    
    return df
def run_fuzzy_dedupe(
    main_df: pd.DataFrame,
    cols: dict,
    dsu: DSU,
    blocks: DataFrameGroupBy,
    main_match_criteria: str,
    match_field: str,
    u_bound: float,
    l_bound: float,
    nickname_col: Optional[str] = None,
) -> pd.DataFrame:

    score_array = np.zeros(len(main_df))

    nn = NickNamer()

    
    matrix_names = [f"{col.split("_")[1].split(':')[0].strip()}" if "_" in col else col for col in cols.keys()]
    nicknames = {}

    # Looping through blocks to fuzzy on. Blocking can be changed in client yaml
    for _, block_df in blocks:
        n = len(block_df)
        if n < 2:
            continue

        final_matrix = np.zeros((n, n))

        # Nickname set for finding name matches between records like "Christina" and "Tina"
        if nickname_col:
            nicknames.update({
                    name: set(nn.nicknames_of(name)) | {name.lower()}
                    for name in str(block_df[nickname_col].unique())
                })

        # Creating a dictionary of matrices. One for each column being fuzzied on. The columns being fuzzied on are normalized columns from the normalize function
        # They are all in the form "clean_originalcolname:contacttype" i.e clean_homephone:phone, except for address which is just clean_address:address,
        # And name columns which are their original column name
        matrices = {name: np.zeros((n, n)) for name in matrix_names}

        # Normalized columns to fuzzy on
        for col, weight in cols.items():

            records = block_df[col].to_list()
            has_value = np.array([pd.notna(v) for v in records])
            both_have_value = has_value[:,None] & has_value[None, :]

            # If nickname column has been passed, seperate fuzzying will be performed on this column
            if col == nickname_col:

                for (i, name_a), (j, name_b) in combinations(enumerate(records), 2):
                    # Doing a lookup on the nickname dictionary to create an intersecting set for names that are eachothers nicknames
                    match = bool(
                        nicknames.get(name_a, set()) & nicknames.get(name_b, set())
                    )

                    # Giving a score of 100 for names that are eachothers nicknames
                    # The main match criteria used later on will mitigate false positives
                    if match:
                        matrices[nickname_col][i, j] = 100
                        #final_matrix[i, j] += 100 * weight

                    # No nickname matches, WRatio will be used to fuzzy match
                    else:
                        score = fuzz.WRatio(name_a, name_b)
                        matrices[nickname_col][i, j] = score
                        #final_matrix[i, j] += score * weight
                matrices[nickname_col] = np.where(both_have_value,matrices[nickname_col], np.nan)
            # Non nickname columns will use the W Ratio for fuzzying
            else:

                scores = process.cdist(records, records, scorer=fuzz.WRatio)
                #final_matrix += scores * weight
                name = f"{col.split("_")[1].split(':')[0].strip()}" if "_" in col else col
                matrices[name] += scores
                matrices[name] = np.where(both_have_value, matrices[name], np.nan)
       
        
            
        mask = np.array([pd.notna(v) for v in matrices.values()])
        total_filled = np.zeros((n,n))
        for m in mask:
            total_filled += np.where(m, 1,0)
    
       
        # Creates a matrix with how many fields return a matching score of over 95.
        # Creating a count for how many fields the two comparing records have in common
        hits = {k: v >= u_bound for k, v in matrices.items()}
        hit_count = sum(v.astype(int) for v in hits.values())
        gate_mask = (matrices[main_match_criteria] >= u_bound) & (hit_count >= 1)
        final_matrix_mask = np.where(gate_mask,True,False)
        for col,weight in cols.items():
            
            res = np.reciprocal(total_filled)
            res +=weight
               
            matrices[f"{col.split("_")[1].split(':')[0].strip()}" if "_" in col else col] *= weight 
        final_matrix = np.where(final_matrix_mask,np.nansum(list(matrices.values()),axis=0),0)  
        


  

        

        # Assign scores to df
        assign_scores(final_matrix, block_df, score_array, dsu, l_bound)

    main_df = assign_match_id(main_df=main_df, dsu=dsu, match_field=match_field)
    label_df(main_df=main_df, score_array=score_array)
    
    return main_df
