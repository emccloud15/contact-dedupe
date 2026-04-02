import yaml
import pandas as pd
from pathlib import Path

from pydantic import ValidationError

from common.logger import get_logger
from common.exceptions import DataLoadError, ConfigError
from common.models import ClientConfig

logger = get_logger(__name__)


# Transform client YAML to Pydantic model Settings
def load_client_config(file_path: str) -> dict:
    if not file_path.endswith((".yaml", ".yml")):
        raise DataLoadError(f"File path is not a yaml file. Please try again")
    try:
        with open(file_path, "r") as f:
            raw_config = yaml.safe_load(f)
    except (FileNotFoundError, OSError) as e:
        raise DataLoadError(f"Failed to load client config file: {file_path}") from e
    try:
        client_settings = ClientConfig(**raw_config)
        logger.info(f"{client_settings.CLIENT_NAME} config loaded")
    except ValidationError as e:
        raise ConfigError(
            f"Invalid client configuration. Check file for labeling errors: {file_path}"
        ) from e
    return client_settings


def load_data_df(file_path: Path) -> pd.DataFrame:
    if file_path.suffix == ".csv":
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError as e:
            raise DataLoadError(f"File path not found: {file_path}") from e
        except Exception as e:
            raise DataLoadError(f"Failed to load csv data file: {file_path}") from e

    else:
        try:
            df = pd.read_excel(file_path, engine="openpyxl")
        except FileNotFoundError as e:
            raise DataLoadError(f"File path not found: {file_path}") from e
        except Exception as e:
            raise DataLoadError(f"Failed to load xlsx data file: {file_path}") from e
    return df
