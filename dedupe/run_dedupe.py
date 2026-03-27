import pandas as pd
import sys

from pydantic import BaseModel

from common.exceptions import ConfigError
from common.utils import load_data_df
from dedupe.normalize import normalize_df
from dedupe.core_dedupe import run_strict_dedupe, run_fuzzy_dedupe
from common.logger import get_logger

logger = get_logger(__name__)


class DSU:
    def __init__(self,n):
        self.parent = list(range(n))
        self.rank = [0] * n
    def find(self, x):
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    def union(self,x,y):
        root_x = self.find(x)
        root_y = self.find(y)
        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1  
        

def column_weights(cleaned_cols: list[str], client_cfg: object) -> dict:
    # Getting the column weights from the client cfg file, combining them with the dataframe column names that will be used for the fuzzy
    # Separate dictionary unioned in becuase the structure of the name weights in the cfg file is different to allow for weights per name column
    return {col: getattr(client_cfg.COLUMNS,col.split(':')[1].strip()).weight for col in cleaned_cols} | {name_col:weight for name_col,weight in client_cfg.COLUMNS.name.weight}
    

def test_weights(weighted_cols: dict):

    total_weight = sum(weighted_cols.values())
    logger.info(f"weight is: {total_weight}")
    
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
                    need = (total_weight - 1.0)/n
                    print(f"{need} will be subtracted from each current weight")
                    fixed_weights = {k:v-need for k,v in weighted_cols.items()}
                    print(f"Final fixed weights: {fixed_weights}")
                    return fixed_weights
                else:
                    need = (1.0 - total_weight)/n
                    print(f"{need} will be added to each current weight")
                    fixed_weights = {k:v+need for k,v in weighted_cols.items()}
                    print(f"Final fixed weights: {fixed_weights}")
                    return fixed_weights
            count +=1
            if count > 4:
                print("Exiting program. Too many incorrect choices")
                logger.info("Too many incorrect choice attempts")
                sys.exit(0)
    return weighted_cols
 


    
    



def run_dedupe(client_cfg: BaseModel) -> None:
    #Load file into df
    original_df = load_data_df(client_cfg.FILE_PATH)

    #Normalize the df
    normalized_df = normalize_df(original_df, client_cfg.COLUMNS)    



    # Only dedupe on normalized columns
    cols = [col for col in normalized_df.columns if col.endswith((':email',':phone',':address'))]

    # Instantiate DSU 
    dsu = DSU(len(normalized_df))

    # Run strict dedupe
    main_df = run_strict_dedupe(normalized_df, cols, dsu)
    

    # Create dictionary for column weights. Specified in client config
    weighted_cols = column_weights(cols,client_cfg)

    # Ensure given weights add to 1.0
    # Gives user choice to auto-balance or exit. THIS FUNCTION CAN EXIT THE PROGRAM
    weighted_cols = test_weights(weighted_cols)

    # Run fuzzy dedupe
    main_df = run_fuzzy_dedupe(main_df, weighted_cols, dsu, client_cfg.BLOCKING, client_cfg.BOUNDS)
 
    main_df['root'] = main_df.index.map(dsu.find)
    main_df['match_id'] = main_df['root'].map(main_df['legacycontactid'])
    main_df.sort_values('match_id').to_csv('after_fuzzy.csv',index=False)
