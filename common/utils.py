import yaml
import pandas as pd

from pydantic import ValidationError

from common.exceptions import DataLoadError, ConfigError
from common.models import ClientConfig, SystemConfig


# Load config for program with client YAML file path
def load_sys_config(file_path: str) -> dict:
    
    try:
        with open(file_path, 'r') as f:
            config = yaml.safe_load(f)
    except DataLoadError as e:
        raise DataLoadError(f"Failed to load program config file: {file_path}") from e
    try:
        system_settings = SystemConfig(**config)
    except ValidationError as e:
        raise ConfigError(f"Invalid program configuration. Check file for labeling errors: {file_path}") from e
    return system_settings
    

# Transform client YAML to Pydantic model Settings
def load_client_config(file_path: str):
    try:
        with open(file_path, 'r') as f:
            raw_config = yaml.safe_load(f)
    except DataLoadError as e:
        raise DataLoadError(f"Failed to load client config file: {file_path}") from e
    try:
        client_settings = ClientConfig(**raw_config)
    except ValidationError as e:
        raise ConfigError(f"Invalid client configuration. Check file for labeling errors: {file_path}") from e
    return client_settings

def load_data_df(file_path: str) -> pd.DataFrame:
    if file_path.suffix == ".csv":
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            raise DataLoadError(f"Failed to load csv data file: {file_path}") from e
            
    else:
        try:
            df = pd.read_excel(file_path, engine='openpyxl')
        except Exception as e:
            raise DataLoadError(f"Failed to load xlsx data file: {file_path}") from e
    return df