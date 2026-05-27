import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
import sys
from pathlib import Path
from datetime import datetime

from dedupe.dsu import DSU
from dedupe.normalize import normalize_df
from dedupe.core_dedupe import run_strict_dedupe, run_fuzzy_dedupe

from common.exceptions import ConfigError
from common.utils import load_data_df
from common.logger import get_logger
from common.models import ClientConfig, Blocking

logger = get_logger(__name__)
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

def create_check_name_matches_file(df: pd.DataFrame, output_path: str) -> None:
    name_cols = [col for col in df.columns if (col.startswith('clean_')) & ('Name' in col)]
    if name_cols:
        mask = (df[name_cols].all(axis=1)) & (df['score'] == 0)
        df = df.loc[mask]
        cols = create_check_cols(list(df.columns))
        cols = [col for col in cols if not col.startswith('clean') | ('score' in col) | ('match_id' in col)]
        df = df[cols]
        mask = df['Name'] != 'Friends of GCM'
        df = df.loc[mask]

        df = df.merge(df, how='inner', left_on='Name', right_on='Name', suffixes=('_main','_duplicate'))
        df = df[df['Id_main'] != df['Id_duplicate']]
        df = df[~df.apply(lambda x: frozenset([x['Id_main'], x['Id_duplicate']]), axis=1).duplicated()] #type: ignore

        df.insert(0, 'Merge', 'CHECK', allow_duplicates=True)
        df.sort_values('Name').to_csv(output_path, index=False)
    

def create_col_list(columns: list[str], contact_type: str, cols: list) -> list[str]:

    name_cols = [col for col in columns if col.endswith(f":name_{contact_type}")]
    if name_cols:
        cols += name_cols
    else:
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

        print("Total weight needs to equal 1.0")
        print("1. Exit Program\n2. Continue with auto-balanced default weights")
        count = 0
        choice = 1
        while choice != 1 or choice != 2 or count > 4:
            choice = int(input("Please type 1 or 2 to continue: \n"))
            if choice == 1:
                logger.info("Program exited due to incorrect weights")
                sys.exit(0)
            else:
                n = len(weighted_cols)
                if total_weight > 1.0:
                    need = (total_weight - 1.0) / n
                    print(f"{need} will be subtracted from each current weight")
                    fixed_weights = {k: v - need for k, v in weighted_cols.items()}
                    print(f"Final fixed weights: {fixed_weights}")
                    return fixed_weights
                else:
                    need = (1.0 - total_weight) / n
                    print(f"{need} will be added to each current weight")
                    fixed_weights = {k: v + need for k, v in weighted_cols.items()}
                    print(f"Final fixed weights: {fixed_weights}")
                    return fixed_weights
            count += 1
            if count > 4:
                print("Exiting program. Too many incorrect choices")
                logger.info("Too many incorrect choice attempts")
                sys.exit(0)
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

def virtuous_table_setup(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:,'idx'] = df.index
    df.loc[:,'Duplicate idx'] = df.index
    df.loc[:,'order'] = 1
    df.loc[:,'Duplicate order'] = 2
    primary_cols = [col for col in df.columns if 'Duplicate' not in col]
    duplicate_cols = ['Duplicate ' + col for col in primary_cols]


    primary_df = df[primary_cols]
    duplicate_df = df[duplicate_cols]

    duplicate_df.columns = primary_df.columns
    return pd.concat([primary_df, duplicate_df], ignore_index=True)
    

def create_final_file(
    df: pd.DataFrame,
    output_path: Path,
    client_name: str,
    u_bound: float,
    l_bound: float,
    virtuous: bool
) -> None:
    
    today = datetime.today().date()
    df['score'] = df['score'].round(0)

    # Master file output
    df.sort_values("match_id").to_csv(
        f"{output_path}/{client_name}_master_{today}.csv", index=False
    )

    # Check file output
    check_output_path = f"{output_path}/{client_name}_check_{today}.csv"
    name_check_output_path = f"{output_path}/{client_name}_name_check_{today}.csv"
    create_check_file(df, check_output_path, u_bound)
    create_check_name_matches_file(df, name_check_output_path)

    # Virtuous file output
    if virtuous:
        
        primary_df = df[df['order'] == 1]
        comparative_df = df[df['order'] == 2]
        
    
        compared_record_cols = [f"Duplicate {col}" for col in df.columns]
        comparative_df.columns = compared_record_cols

        primary_df = primary_df.set_index('idx')
        comparative_df = comparative_df.set_index('Duplicate idx')
        

        final_df = pd.concat([primary_df,comparative_df], axis=1)

        # This labels rows where 1 and only one value matched. If a record only matched on email they should be given a look
        duplicate_cols = [col for col in compared_record_cols if col.endswith('_dupe')]
        mask = (final_df['Duplicate score'] == 0) & (final_df[duplicate_cols].sum(axis=1) == 1)
        final_df.loc[mask, 'Duplicate score'] = 45

        # Same as previous but at least 2 matched
        mask = (final_df['Duplicate score'] == 0) & (final_df[duplicate_cols].sum(axis=1) > 1)
        final_df.loc[mask, 'Duplicate score'] = 55

        mask = (final_df['Duplicate score'] <= u_bound) & (final_df['Duplicate score'] >= l_bound)
        final_df.loc[mask,'Duplicate dupe'] = 'CHECK'
        mask = (final_df['Duplicate score'] < l_bound)
        final_df.loc[mask,'Duplicate dupe'] = 'IGNORE'
        mask = (final_df['Duplicate score'] > u_bound)
        final_df.loc[mask, 'Duplicate dupe'] = 'MERGE'

        
        cols_to_drop = [col for col in final_df.columns if (col.startswith('clean_')) | 
                        (col in ['dupe', 'score', 'match_id']) |
                         ('root' in col) |
                         ('count' in col) |
                         ('order' in col) |
                         ((col.startswith('Duplicate clean_')) & (not col.endswith('dupe')))
                           ] 
        final_df = final_df.rename(columns={'Duplicate dupe':'Merge'}).drop(columns=cols_to_drop)
        final_df.to_csv(f"{output_path}/{client_name}_virtuous_{today}.csv", index=False)






def run_dedupe(client_cfg: ClientConfig, input_path: Path, output_path: Path) -> None:

    # Load file into df
    original_df = load_data_df(input_path)


    if client_cfg.VIRTUOUS:
        original_df = virtuous_table_setup(original_df)

    # Normalize the df
    normalized_df = normalize_df(original_df, client_cfg.COLUMNS)
    dsu = DSU(len(normalized_df))
   

    # Primary columns are normalized columns usually combined with names for strict dedupe
    cols = []
    contact_types = ['address','email','phone','name']
    for ct in contact_types:
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
        for _, group_df in group:
            results.append(run_strict_dedupe(group_df, cols, dsu, client_cfg.MATCH_FIELD))

        result_df = pd.concat(results)
        normalized_df.loc[result_df.index, temp_cols] = result_df[temp_cols]
        main_df = normalized_df
    else:
        main_df = run_strict_dedupe(normalized_df, cols, dsu, client_cfg.MATCH_FIELD)

    # Handle column set up for fuzzy
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

    create_final_file(
        df=main_df,
        output_path=output_path,
        client_name=client_cfg.CLIENT_NAME,
        u_bound=client_cfg.BOUNDS.u_bound,
        l_bound=client_cfg.BOUNDS.l_bound,
        virtuous=client_cfg.VIRTUOUS
    )

    logger.info("Dedupe complete")
