import sys
import click
from pathlib import Path
from datetime import datetime

from contact_dedupe.common.utils import Utilities
from contact_dedupe.common.logger import get_logger
from contact_dedupe.dedupe.core import Dedupe, VirtuousDedupe
from contact_dedupe.common.final_files import create_final_file, create_virtuous_file
from contact_dedupe.common.exceptions import DataLoadError, ConfigError

logger = get_logger(__name__)


class CleanPath(click.Path):
    def convert(self, value, param, ctx):
        value = str(value).strip("'").strip('"')
        return super().convert(value,param,ctx)
    

@click.command()
@click.option("--dir", type=CleanPath(exists=True), default=None, required=True, help="Directory containing file to be deduped, and the yaml config")
def main(dir):
    try:
        yaml_file, dupe_file = Utilities.load_data_from_dir(Path(dir))
        client_config = Utilities.load_client_config(yaml_file)
        dupe_df = Utilities.load_data_df(dupe_file)
        output_path = Path(dir).resolve().parent / f"Output_{client_config.CLIENT_NAME}_{datetime.today().date()}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        if client_config.VIRTUOUS:
             = VirtuousDedupe(client_cfg=client_config, df=dupe_df)
            deduped_df = main_df.run()
            contact_type_df = main_df
            create_virtuous_file(df=deduped_df, contact_type_df=, output_dir=output_path, u_bound=client_config.BOUNDS.u_bound, l_bound=client_config.BOUNDS.l_bound)

        else:
            main_df = Dedupe(client_cfg=client_config, df=dupe_df)
            main_df.run()

        logger.info("Dedupe complete")

    except DataLoadError as e:
        logger.exception(f"Dedupe failed during loading data: {e}")
        sys.exit(1)
    except ConfigError as e:
        logger.exception(f"Config file error: {e}")
        sys.exit(1)
    except KeyboardInterrupt as e:
        click.echo(f"{e}\nExiting...")


if __name__ == "__main__":
    main()
