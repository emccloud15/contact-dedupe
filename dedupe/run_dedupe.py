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


def column_weights(cleaned_cols: list[str], client_cfg: ClientConfig) -> dict:
    # Getting the column weights from the client cfg file, combining them with the dataframe column names that will be used for the fuzzy
    # Separate dictionary unioned in becuase the structure of the name weights in the cfg file is different to allow for weights per name column
    return {
        col: getattr(client_cfg.COLUMNS, col.split(":")[1].strip()).weight
        for col in cleaned_cols
    } | {name_col: weight for name_col, weight in client_cfg.COLUMNS.name.weight}


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
def create_block(main_df: pd.DataFrame, blocking_rules: Blocking, fuzzy: bool) -> DataFrameGroupBy:
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


def create_final_file(
    df: pd.DataFrame,
    original_cols: list[str],
    dsu: DSU,
    match_field: str,
    output_path: Path,
    client_name: str,
) -> None:
    df["root"] = df.index.map(dsu.find)
    try:
        df["match_id"] = df["root"].map(df[match_field])
    except KeyError:
        raise ConfigError(
            f"{match_field} not found in csv file columns. Check MATCH_FIELD assignment in the client yaml."
        )
    today = datetime.today().date()

    # Master file output
    df.sort_values("match_id").to_csv(
        f"{output_path}/{client_name}_master_{today}.csv", index=False
    )

    # Check file output
    check_ids = df[df["dupe"] == "CHECK"]["root"].to_list()
    df[df["root"].isin(check_ids)].sort_values("match_id").to_csv(
        f"{output_path}/{client_name}_check_{today}.csv", index=False
    )

    # Cleaned output
    original_cols.append("match_id")
    df[df["count"] == 1][original_cols].sort_values("match_id").to_csv(
        f"{output_path}/{client_name}_cleaned_{today}.csv", index=False
    )


def run_dedupe(client_cfg: ClientConfig) -> None:

    # Load file into df
    original_df = load_data_df(client_cfg.FILE_PATH)
    original_cols = original_df.columns.to_list()

    # Normalize the df
    normalized_df = normalize_df(original_df, client_cfg.COLUMNS)
    dsu = DSU(len(normalized_df))

    # Primary columns are normalized columns usually combined with names for strict dedupe
    primary = [
        col
        for col in normalized_df.columns
        if col.endswith((":name_email", ":name_phone", ":name_address"))
    ]
    # Secondary columns are non name combined normalized columns for fuzzy
    secondary = [
        col
        for col in normalized_df.columns
        if col.endswith((":email", ":phone", ":address"))
    ]
    cols = primary if primary else secondary

    # Group for client required strict group deduping
    try:
        group = create_block(normalized_df, client_cfg.BLOCKING, False)
    except KeyError:
        raise ConfigError(
            f"BLOCKING column {client_cfg.BLOCKING.column} is not in the csv file. Please fix in the yaml"
        )

    # Run strict dedupe
    if client_cfg.GROUP_BY:
        temp_cols = cols + [f"{col}_dupe" for col in cols] + ["dupe", "score", "count"]
        results = []
        for _, group_df in group:
            results.append(run_strict_dedupe(group_df, cols, dsu))

        result_df = pd.concat(results)
        normalized_df.loc[result_df.index, temp_cols] = result_df[temp_cols]
        main_df = normalized_df
    else:
        main_df = run_strict_dedupe(normalized_df, cols, dsu)

    # Handle column set up for fuzzy
    cols = secondary
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
        nickname_col=client_cfg.NICKNAME,
    )

    create_final_file(
        df=main_df,
        original_cols=original_cols,
        dsu=dsu,
        match_field=client_cfg.MATCH_FIELD,
        output_path=client_cfg.OUTPUT_PATH,
        client_name=client_cfg.CLIENT_NAME,
    )

    logger.info("Dedupe complete")
