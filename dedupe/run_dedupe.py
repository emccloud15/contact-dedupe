import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
import sys
import click
import questionary

from typing import Tuple



from dedupe.dsu import DSU
from dedupe.normalize import normalize_df
from dedupe.core_dedupe import run_strict_dedupe, run_fuzzy_dedupe

from common.exceptions import ConfigError
from common.utils import load_data_df
from common.logger import get_logger
from common.models import ClientConfig, Blocking

logger = get_logger(__name__)
  
def create_col_list(columns: list[str], contact_type: str, cols: list) -> list[str]:

    name_cols = [col for col in columns if col.endswith(f":name_{contact_type}")]
    if name_cols:
        cols += name_cols
    else:
        # The type column is for contact type specifically for the virtuous dedupe version
        cols += [col for col in columns if col.endswith(f":{contact_type}")]

    return cols

def column_weights(cleaned_cols: list[str], client_cfg: ClientConfig) -> dict:
    # Getting the column weights from the client cfg file, combining them with the dataframe column names that will be used for the fuzzy
    # Separate dictionary unioned in becuase the structure of the name weights in the cfg file is different to allow for weights per name column
    weights = {}
    for col in cleaned_cols:
        
        for attr,field in getattr(client_cfg.COLUMNS, col.split(":")[1].strip()):
            if attr == 'weight':
                weights.update({col: field} if isinstance(field, float) else {col:item[1] for item in field if item[0] in col})
             
    return weights

# Gives user choice to auto-balance or exit. THIS FUNCTION CAN EXIT THE PROGRAM
def test_weights(weighted_cols: dict) -> dict:

    total_weight = sum(weighted_cols.values())

    if total_weight != 1.0:
        print("Total fuzzy column weights need to sum to 1.0")
        choice = questionary.confirm(message="Continue with auto-balanced weight?").ask()
        if choice:
            n = len(weighted_cols)
            if total_weight > 1.0:
                need = (total_weight - 1.0) / n
                click.echo(f"{need} will be subtracted from each current weight")
                fixed_weights = {k: v - need for k, v in weighted_cols.items()}
                click.echo(f"Final fixed weights: {fixed_weights}")
                return fixed_weights
            else:
                need = (1.0 - total_weight) / n
                click.echo(f"{need} will be added to each current weight")
                fixed_weights = {k: v + need for k, v in weighted_cols.items()}
                click.echo(f"Final fixed weights: {fixed_weights}")
                return fixed_weights
        else:
            raise KeyboardInterrupt("Column weights in the yaml file must sum to 1. Fix to continue")
    else:
        return weighted_cols            

# Create either grouping for entire dedupe process or blocking for fuzzy only
def create_block(
    main_df: pd.DataFrame, blocking_rules: Blocking, fuzzy: bool
) -> DataFrameGroupBy:
    if fuzzy:
        dupe_df = main_df[main_df["count"] == 1]
    else:
        dupe_df = main_df

    if blocking_rules.portion is None or blocking_rules.type.lower() == "state":
        return dupe_df.groupby(dupe_df[blocking_rules.column])

    else:
        if blocking_rules.portion.lower() == "start":
            return dupe_df.groupby(dupe_df[blocking_rules.column].str[:3])
        else:
            return dupe_df.groupby(dupe_df[blocking_rules.column].str[-3:])

# Make the virtuous data health tools csv output into a format for this dedupe tool.
# unpivots side by side records into all records stacked 
def virtuous_table_setup(df: pd.DataFrame, contact_type: bool) -> Tuple[pd.DataFrame, pd.DataFrame]:
    try:
        print(len(df))
        # The virtuous output file names all the duplicate fields starting with 'Duplicate' except for the legacy id field. They name that 'Legacy Duplicate Id' 
        df = df.rename(columns={'Legacy Duplicate Id': 'Duplicate Legacy Id'})
    except:
        pass
    df.loc[:,'Merge'] = "CHECK"
    if contact_type:
        
        mask = (df['Type'] != df['Duplicate Type'])
        df.loc[mask,'Merge'] = "IGNORE"
    
    mask = df['Merge'] == "CHECK"
    dupe_df = df.loc[mask].reset_index(drop=True).drop(columns='Merge')
    virtuous_contact_type_df = df.loc[~mask]
    
    dupe_df.loc[:,'idx'] = dupe_df.index
    dupe_df.loc[:,'Duplicate idx'] = dupe_df.index
    dupe_df.loc[:,'order'] = 1
    dupe_df.loc[:,'Duplicate order'] = 2
    dupe_df.loc[:, 'Duplicate Match Qualifiers'] = dupe_df.loc[:, 'Match Qualifiers']
    primary_cols = [col for col in dupe_df.columns if 'Duplicate' not in col]
    duplicate_cols = ['Duplicate ' + col for col in primary_cols]
    primary_df = dupe_df.loc[:,primary_cols]
    duplicate_df = dupe_df.loc[:,duplicate_cols]
    duplicate_df.columns = primary_df.columns

    main_df = pd.concat([primary_df, duplicate_df], ignore_index=True)
    
    return main_df, virtuous_contact_type_df
    

def run_dedupe(client_cfg: ClientConfig, original_df: pd.DataFrame) -> pd.DataFrame:


    # Takes the virtuous health tool download
    # if client_cfg.VIRTUOUS:
    #     original_df, virtuous_contact_type_df = virtuous_table_setup(original_df, client_cfg.VIRTUOUS.contact_type)
    # else:
    #     virtuous_contact_type_df = None

    # Normalize the df
    normalized_df = normalize_df(original_df, client_cfg.COLUMNS)
    dsu = DSU(len(normalized_df))
    
   

    # Primary columns are normalized columns usually combined with names for strict dedupe
    cols = []
    contact_types = ['address','email','phone','name']
    with click.progressbar(contact_types, label="creating combined columns") as bar:
        for ct in bar:
            cols = create_col_list(columns=normalized_df.columns.to_list(), contact_type=ct, cols=cols)
            
            

    # Group for client required strict group deduping
    try:
        group = create_block(normalized_df, client_cfg.BLOCKING, False)
    except KeyError:
        raise ConfigError(
            f"BLOCKING column {client_cfg.BLOCKING.column} is not in the csv file. Please fix in the yaml"
        )

    # Run strict dedupe
    if client_cfg.BLOCKING.strict:
        temp_cols = cols + [f"{col}_dupe" for col in cols] + ["dupe", "score", "count"]
        results = []
        with click.progressbar(length=len(group), label="strict deduping") as bar:
            for _, group_df in group:
                result = run_strict_dedupe(group_df, cols, dsu, client_cfg.MATCH_FIELD)
                results.append(result)
                bar.update(1)

        result_df = pd.concat(results)
        normalized_df.loc[result_df.index, temp_cols] = result_df[temp_cols]
        main_df = normalized_df

    else:
        with click.progressbar(length=len(normalized_df), label="strict deduping") as bar:
            main_df = run_strict_dedupe(normalized_df, cols, dsu, client_cfg.MATCH_FIELD)

    # Handle column set up for fuzzy
    # The duplicate type & type columns are for the virtuous specific dedupe.
    cols = [col for col in normalized_df.columns if col.endswith((':address',':email',':phone',':name'))]

    # Create dictionary for column weights. Specified in client config
    weighted_cols = column_weights(cols, client_cfg)
    
    # Ensure given weights add to 1.0
    weighted_cols = test_weights(weighted_cols)
  

    # Run fuzzy dedupe
    blocks = create_block(main_df, client_cfg.BLOCKING, True)
    main_df = run_fuzzy_dedupe(
        main_df=main_df,
        cols=weighted_cols,
        dsu=dsu,
        blocks=blocks,
        u_bound=client_cfg.BOUNDS.u_bound,
        l_bound=client_cfg.BOUNDS.l_bound,
        main_match_criteria=client_cfg.MAIN_MATCH_CRITERIA,
        match_field=client_cfg.MATCH_FIELD,
        nickname_col=client_cfg.NICKNAME,
    )
    # if virtuous_contact_type_df is not None:
    #     return main_df
        
    
    return main_df
    
