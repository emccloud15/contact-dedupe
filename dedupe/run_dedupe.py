import pandas as pd

from pydantic import BaseModel

from common.utils import load_data_df
from dedupe.normalize import normalize_df


def run_dedupe(client_cfg: BaseModel) -> None:
    #Load file into df
    original_df = load_data_df(client_cfg.FILE_PATH)

    #Normalize the df
    normalized_df = normalize_df(original_df, client_cfg.COLUMNS)
    normalized_df.to_csv('normalized.csv',index=False)
