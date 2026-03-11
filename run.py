import sys
from common.utils import load_sys_config, load_client_config
from common.logger import get_logger
from dedupe.run_dedupe import run_dedupe
from common.exceptions import DataLoadError, ConfigError

logger = get_logger(__name__)

def main():
    try:
        system_config = load_sys_config('settings.yaml')
        client_config = load_client_config(system_config.CLIENT_YAML)
        run_dedupe(client_config)
    except DataLoadError as e:
        logger.exception(f"Dedupe failed during loading data: {e}")
        sys.exit(1)
    except ConfigError as e:
        logger.exception(f"Config file error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
