import pandas as pd
import numpy as np
from pandas.core.groupby import DataFrameGroupBy
from numpy.typing import NDArray
import click
import questionary


from nicknames import NickNamer
from itertools import combinations
from rapidfuzz import process, fuzz


from .dsu import DSU
from .normalize import normalize_df

from contact_dedupe.common.exceptions import ConfigError
from contact_dedupe.common.logger import get_logger
from contact_dedupe.common.models import ClientConfig



logger = get_logger(__name__)



class Dedupe:
    def __init__(self, client_cfg: ClientConfig, df: pd.DataFrame) -> None:
        self.client_cfg = client_cfg
        self.portion = self.client_cfg.BLOCKING.portion
        self.blocking = self.client_cfg.BLOCKING.column
        self.nickname_col = self.client_cfg.NICKNAME
        self.u_bound = self.client_cfg.BOUNDS.u_bound
        self.l_bound = self.client_cfg.BOUNDS.l_bound
        self.main_match_criteria = self.client_cfg.MAIN_MATCH_CRITERIA
        self.match_field = self.client_cfg.MATCH_FIELD

        self.original_df = df
        self.contact_types = [field for field,value in self.client_cfg.COLUMNS if value]
        self.strict_dedupe_cols = []
        self.fuzzy_dedupe_cols = []
        # cols that will be fuzzied on as keys and their weights as values
        self.fuzzy_dedupe_col_weights = {}

    # Columns to be deduped on
    def _create_dedupe_col_list(self) -> None:
        for contact_type in self.contact_types:
            name_cols = [col for col in self.main_df.columns if col.endswith(f":name_{contact_type}")]
            if name_cols:
                self.strict_dedupe_cols += name_cols
            else:
                self.strict_dedupe_cols += [col for col in self.main_df.columns if col.endswith(f":{contact_type}")]
    # Creates a hashmap of modifed column names to original column weights set in config
    def _column_weights(self) -> None:
    # Getting the column weights from the client cfg file, combining them with the dataframe column names that will be used for the fuzzy
    # Separate dictionary unioned in becuase the structure of the name weights in the cfg file is different to allow for weights per name column
        for col in self.fuzzy_dedupe_cols:
            for attr,field in getattr(self.client_cfg.COLUMNS, col.split(":")[1].strip()):
                if attr == 'weight':
                    self.fuzzy_dedupe_col_weights.update({col: field} if isinstance(field, float) else {col:item[1] for item in field if item[0] in col})
    # Gives user choice to auto-balance fuzzy column weights or exit
    def _test_weights(self) -> None:

        total_weight = sum(self.fuzzy_dedupe_col_weights.values())

        if total_weight != 1.0:
            print("Total fuzzy column weights need to sum to 1.0")
            choice = questionary.confirm(message="Continue with auto-balanced weight?").ask()
            if choice:
                n = len(self.fuzzy_dedupe_col_weights)
                if total_weight > 1.0:
                    need = (total_weight - 1.0) / n
                    click.echo(f"{need} will be subtracted from each current weight")
                    self.fuzzy_dedupe_col_weights.update({k: v - need for k, v in self.fuzzy_dedupe_col_weights.items()})
                    click.echo(f"Final fixed weights: {self.fuzzy_dedupe_col_weights}")
                else:
                    need = (1.0 - total_weight) / n
                    click.echo(f"{need} will be added to each current weight")
                    self.fuzzy_dedupe_col_weights.update({k: v + need for k, v in self.fuzzy_dedupe_col_weights.items()})
                    click.echo(f"Final fixed weights: {self.fuzzy_dedupe_col_weights}")
            else:
                raise KeyboardInterrupt("Column weights in the yaml file must sum to 1. Fix to continue")
  
    # Create either grouping for entire dedupe process or blocking for fuzzy only
    def _create_block(self, fuzzy: bool) -> DataFrameGroupBy:
        if fuzzy:
            dupe_df = self.main_df[self.main_df["count"] == 1]
        else:
            dupe_df = self.main_df

        if self.portion:
            if self.portion.lower() == "start":
                return dupe_df.groupby(dupe_df[self.blocking].str[:3])
            else:
                return dupe_df.groupby(dupe_df[self.blocking].str[-3:])
        else:
            return dupe_df.groupby(dupe_df[self.blocking])
    
    def _label_df(self, score_array: NDArray) -> None:
        conditions =[
            self.main_df['dupe'] == True,
            score_array > 0
        ]
        choices = [
            100,
            score_array
        ]
        self.main_df['score'] = np.select(condlist=conditions, choicelist=choices, default=0)
        self.main_df.loc[self.main_df['score']==0, 'dupe'] = False
        mask = self.main_df['score'] > self.u_bound
        self.main_df.loc[mask,'dupe'] = True

    def _assign_match_id(self, group_df: pd.DataFrame) -> pd.DataFrame:
        group_df['root'] = group_df.index.map(self.dsu.find)
        try:
            group_df["match_id"] = group_df["root"].map(group_df[self.client_cfg.MATCH_FIELD])
        except KeyError:
            raise ConfigError(
                f"{self.client_cfg.MATCH_FIELD} not found in csv file columns. Check MATCH_FIELD assignment in the client yaml."
            )
        return group_df
    
    def _assign_scores(self, final_matrix: np.ndarray, block_df: pd.DataFrame, score_array: NDArray) -> None:

        upper = np.triu(final_matrix, k=1)
        pairs = np.argwhere(upper >= self.l_bound)
        if len(pairs) < 1:
            return

        for i, j in pairs:
            self.dsu.union(block_df.index[i], block_df.index[j])

        rows, columns = pairs[:, 0], pairs[:, 1]
        scores = upper[rows, columns]

        np.maximum.at(score_array, block_df.index[rows], scores)
        np.maximum.at(score_array, block_df.index[columns], scores)

    def _strict_dedupe(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, "dupe"] = pd.NA
        df.loc[:, "score"] = pd.NA

        # Dedupe on each of the client chosen normalized columns
        for col in self.strict_dedupe_cols:
            mask = df[col].duplicated(keep=False) & df[col].notna()
            df.loc[mask, f"{col}_dupe"] = True
            df.loc[~mask, f"{col}_dupe"] = False
        
        dupe_cols = [f"{col}_dupe" for col in self.strict_dedupe_cols]
        mask = (df[dupe_cols] == True).sum(axis=1) >=3
        df.loc[mask, 'dupe'] = True
        df.loc[mask,"combined_cols"] = df.apply(lambda row: "|".join([row[col] for col in self.strict_dedupe_cols if row[f"{col}_dupe"] == True]), axis=1)
        
        # DSU to link multiple dupes to one root record
        for idx_group in df.groupby("combined_cols").indices.values():
                if len(idx_group) > 1:
                    label_indices = df.index[idx_group]
                    for i in range(1, len(label_indices)):
                        self.dsu.union(label_indices[0], label_indices[i])
        
        df = self._assign_match_id(group_df=df)
        if self.client_cfg.BLOCKING.type == 'id':
            print("count should be 1")
            df['count'] = 1
        else:
            print("Count will not be just 1")
            df["count"] = df.groupby("match_id").cumcount() + 1
        return df
    
    def run_strict_dedupe(self) -> None:

        if self.client_cfg.BLOCKING.strict:
            # If client choses strict deduping this will create those blocks for the strict dedupe method
            try:
                group = self._create_block(False)
            except KeyError:
                raise ConfigError(
                    f"BLOCKING column {self.client_cfg.BLOCKING.column} is not in the csv file. Please fix in the yaml"
                )

            # Adding extra _dupe columns to label T/F in both strict/fuzzy if that cleaned column was found to be a duplicate. adding the dupe,score,count columns to help modify those
            # Columns in the normalized df
            temp_cols = self.strict_dedupe_cols + [f"{col}_dupe" for col in self.strict_dedupe_cols] + ["dupe", "score", "count"]
            results = []
            with click.progressbar(length=len(group), label="strict deduping") as bar:
                for _, group_df in group:
                    result = self._strict_dedupe(group_df)
                    results.append(result)
                    bar.update(1)

            result_df = pd.concat(results)
            # Assigning the updated values in _dupe score and count columns back to the normlaized main df
            self.main_df.loc[result_df.index, temp_cols] = result_df[temp_cols]
        else:
            with click.progressbar(length=len(self.main_df), label="strict deduping") as bar:
                self.main_df = self._strict_dedupe(self.main_df)

    def run_fuzzy_dedupe(self) -> None:

        score_array = np.zeros(len(self.main_df))

        nn = NickNamer()
        nicknames = {}

        blocks = self._create_block(True)
        matrix_names = [f"{col.split("_")[1].split(':')[0].strip()}" if "_" in col else col for col in self.fuzzy_dedupe_col_weights.keys()]
        

        # Looping through blocks to fuzzy on. Blocking can be changed in client yaml
        with click.progressbar(blocks, label="fuzzy matching") as bar:
            for _, block_df in bar:
                n = len(block_df)
                if n < 2:
                    continue

                final_matrix = np.zeros((n, n))

                # Nickname set for finding name matches between records like "Christina" and "Tina"
                if self.nickname_col:
                    nicknames.update({
                            name: set(nn.nicknames_of(name)) | {name.lower()}
                            for name in str(block_df[self.nickname_col].unique())
                        })

                # Creating a dictionary of matrices. One for each column being fuzzied on. The columns being fuzzied on are normalized columns from the normalize function
                # They are all in the form "clean_originalcolname:contacttype" i.e clean_homephone:phone, except for address which is just clean_address:address,
                # And name columns which are their original column name
                matrices = {name: np.zeros((n, n)) for name in matrix_names}

                # Normalized columns to fuzzy on
                for col, weight in self.fuzzy_dedupe_col_weights.items():

                    records = block_df[col].to_list()
                    # Creating a boolean NxN matrix for whether or not a record for the current column is nan
                    has_value = np.array([pd.notna(v) for v in records])
                    both_have_value = has_value[:,None] & has_value[None, :]

                    # If nickname column has been passed, seperate fuzzying will be performed on this column
                    if col == self.nickname_col:

                        for (i, name_a), (j, name_b) in combinations(enumerate(records), 2):
                            # Doing a lookup on the nickname dictionary to create an intersecting set for names that are eachothers nicknames
                            match = bool(
                                nicknames.get(name_a, set()) & nicknames.get(name_b, set())
                            )

                            # Names with an intesecting set of nicknames > 0 are given a score of 100
                            # The main match criteria used later on will mitigate false positives
                            if match:
                                matrices[self.nickname_col][i, j] = 100
                                

                            # No nickname matches, WRatio will be used to fuzzy match
                            else:
                                score = fuzz.WRatio(name_a, name_b)
                                matrices[self.nickname_col][i, j] = score
                        matrices[self.nickname_col] = np.where(both_have_value,matrices[self.nickname_col], np.nan)
                    
                    
                    else:

                        scores = process.cdist(records, records, scorer=fuzz.WRatio)
                        name = f"{col.split("_")[1].split(':')[0].strip()}" if "_" in col else col
                        matrices[name] += scores
                        matrices[name] = np.where(both_have_value, matrices[name], np.nan)
            
                
                # For every specific column score matrix in matrices if both columns were present to be fuzzy matched will receive a value of 1
                # Each matrix will be added together to determine how many fields were not nan values. This is then used to redistribute the weights.    
                mask = np.array([pd.notna(v) for v in matrices.values()])
                total_filled = np.zeros((n,n))
                for m in mask:
                    total_filled += np.where(m, 1,0)
            
            
                # Creates a matrix with how many fields return a matching score of over 95.
                # Creating a count for how many fields the two comparing records have in common
                hits = {k: v >= self.u_bound for k, v in matrices.items()}
                hit_count = sum(v.astype(int) for v in hits.values())
                gate_mask = (matrices[self.main_match_criteria] >= self.u_bound) & (hit_count >= 1)
                final_matrix_mask = np.where(gate_mask,True,False)
                
                for col,weight in self.fuzzy_dedupe_col_weights.items():
                    
                    res = np.reciprocal(total_filled)
                    res +=weight
                    
                    matrices[f"{col.split("_")[1].split(':')[0].strip()}" if "_" in col else col] *= weight 
                final_matrix = np.where(final_matrix_mask,np.nansum(list(matrices.values()),axis=0),0)  
            
                
                # Assign scores to df and apply dsu
                self._assign_scores(final_matrix, block_df, score_array)

                self._assign_match_id(self.main_df)
        self._label_df(score_array=score_array)
        
    
    def run(self) -> pd.DataFrame:

        # Normalize the dataframe when instance is created
        self.main_df = normalize_df(df=self.original_df, data=self.client_cfg.COLUMNS, contact_types=self.contact_types)
        self.dsu = DSU(len(self.main_df)) 

        # We only dedupe on these columns - the normalized columns chosen by client in yaml
        self._create_dedupe_col_list()
        
        # Run strict dedupe
        self.run_strict_dedupe()
       
        # We fuzzy on columns without the name attached. So we grab only the cleaned columns without the name attached. 
        self.fuzzy_dedupe_cols = [col for col in self.main_df.columns if col.endswith((':address',':email',':phone',':name'))]

        # Update column weight dictionary. Specified in client config
        self._column_weights()

        # Ensure given weights add to 1.0
        self._test_weights()

        # Run fuzzy dedupe
        self.run_fuzzy_dedupe()

        return self.main_df
    
class VirtuousDedupe(Dedupe):
    def __init__(self, client_cfg: ClientConfig, df: pd.DataFrame, contact_type: bool) -> None:
        super().__init__(client_cfg, df)
        self.contact_type = contact_type

    def _check_contact_type(self) -> None:

        mask = (self.original_df['Type'] != self.original_df['Duplicate Type'])
        self.virtuous_contact_type_df = self.original_df.loc[mask]
        self.virtuous_dupe_df = self.original_df.loc[~mask].reset_index(drop=True)
        
        self.virtuous_contact_type_df.loc[:,'Merge'] = 'IGNORE'
        self.virtuous_contact_type_df.loc[:,'Duplicate score'] = 0
        self.virtuous_contact_type_df.loc[:,'Duplicate match_id'] = self.virtuous_contact_type_df.loc[:,self.client_cfg.MATCH_FIELD]
    
    # Make the virtuous data health tools csv output into a format for this dedupe tool.
    # unpivots side by side records into all records stacked 
    def _table_setup(self):
        try:
            # The virtuous output file names all the duplicate fields starting with 'Duplicate' except for the legacy id field. They name that 'Legacy Duplicate Id' 
            self.virtuous_dupe_df = self.virtuous_dupe_df.rename(columns={'Legacy Duplicate Id': 'Duplicate Legacy Id'})
        except:
            pass
        
        self.virtuous_dupe_df.loc[:,'idx'] = self.virtuous_dupe_df.index
        self.virtuous_dupe_df.loc[:,'Duplicate idx'] = self.virtuous_dupe_df.index
        self.virtuous_dupe_df.loc[:,'order'] = 1
        self.virtuous_dupe_df.loc[:,'Duplicate order'] = 2
        self.virtuous_dupe_df.loc[:, 'Duplicate Match Qualifiers'] = self.virtuous_dupe_df.loc[:, 'Match Qualifiers']
        primary_cols = [col for col in self.virtuous_dupe_df.columns if 'Duplicate' not in col]
        duplicate_cols = ['Duplicate ' + col for col in primary_cols]
        primary_df = self.virtuous_dupe_df.loc[:,primary_cols]
        duplicate_df = self.virtuous_dupe_df.loc[:,duplicate_cols]
        duplicate_df.columns = primary_df.columns

        self.virtuous_dupe_df = pd.concat([primary_df, duplicate_df], ignore_index=True)
        
   

       
    def run(self) -> pd.DataFrame:
        
        # If config has contact_type option as true, we will completely ignore any contact records with mismatching contact types and remove them early.
        if self.contact_type:
            self._check_contact_type()
        else:
            self.virtuous_dupe_df = self.original_df

        # Reformat virtuous df into dedupe formatted df
        self._table_setup()

        # Clean & combine columns
        self.main_df = normalize_df(df=self.virtuous_dupe_df, data=self.client_cfg.COLUMNS, contact_types=self.contact_types)
        self.dsu = DSU(len(self.main_df))

        # We only dedupe on these columns - the normalized columns chosen by client in yaml
        self._create_dedupe_col_list()
        
        # Run strict dedupe
        self.run_strict_dedupe()
       
        # We fuzzy on columns without the name attached. So we grab only the cleaned columns without the name attached. 
        self.fuzzy_dedupe_cols = [col for col in self.main_df.columns if col.endswith((':address',':email',':phone',':name'))]

        # Update column weight dictionary. Specified in client config
        self._column_weights()

        # Ensure given weights add to 1.0
        self._test_weights()

        # Run fuzzy dedupe
        self.run_fuzzy_dedupe()

        return self.main_df


          




    

