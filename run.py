import sys
import argparse


from common.utils import load_client_config
from common.logger import get_logger
from dedupe.run_dedupe import run_dedupe
from common.exceptions import DataLoadError, ConfigError

logger = get_logger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Dedupe Tool")
    parser.add_argument(
        "--yaml", "-y", type=str, required=True, help="Input client yaml path"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        client_config = load_client_config(args.yaml)
        run_dedupe(client_config)
    except DataLoadError as e:
        logger.exception(f"Dedupe failed during loading data: {e}")
        sys.exit(1)
    except ConfigError as e:
        logger.exception(f"Config file error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
