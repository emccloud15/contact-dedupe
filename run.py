import sys
import argparse

from pathlib import Path

from common.utils import load_client_config, load_data_from_dir, load_data_df
from common.logger import get_logger
from dedupe.run_dedupe import run_dedupe
from common.exceptions import DataLoadError, ConfigError

logger = get_logger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Dedupe Tool")
    parser.add_argument(
        "--dir", "-d", type=str, help="Directory that includes the yaml and duplicated file. Output will be located here."
    )
    parser.add_argument(
        "--yaml", "-y", type=str, help="Input client yaml path"
    )
    parser.add_argument(
        "--dupe", type=str, help="File path to be deduplicated"
    )
    parser.add_argument(
        "--output", "-o", type=str, help="Directory for output files"
    )
    
    return parser.parse_args()


def main():

    args = parse_args()
    
    try:
        if args.dir:
            output_path = Path(args.dir)
            output_path.mkdir(parents=True, exist_ok=True)
            yaml_file, dupe_file = load_data_from_dir(output_path)
        else:
            yaml_file = Path(args.yaml)
            dupe_file = Path(args.dupe)
            output_path = Path(args.output)
            output_path.mkdir(parents=True, exist_ok=True)
        client_config = load_client_config(yaml_file)
        dupe_df = load_data_df(dupe_file)
        run_dedupe(client_config, dupe_df, output_path)
    except DataLoadError as e:
        logger.exception(f"Dedupe failed during loading data: {e}")
        sys.exit(1)
    except ConfigError as e:
        logger.exception(f"Config file error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
