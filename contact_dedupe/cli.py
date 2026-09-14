import sys
import click
import questionary
from pathlib import Path
from datetime import datetime

import argparse
from .common.utils import Utilities
from .common.logger import get_logger
from .dedupe.core import Dedupe
from .common.exceptions import DataLoadError, ConfigError

logger = get_logger(__name__)


class CleanPath(click.Path):
    def convert(self, value, param, ctx):
        value = str(value).strip("'").strip('"')
        return super().convert(value,param,ctx)

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dedupe a CSV file based on a YAML config")
    parser.add_argument("--yaml", type=CleanPath(exists=True), help="Path to the YAML config file")
    parser.add_argument("--file", type=CleanPath(exists=True), help="Path to the CSV file to be deduped")
    parser.add_argument("--output", type=CleanPath(), help="Path to the output directory for the deduped CSV file")
    return parser

def choose_file_or_directory(prompt: str, type: str) -> Path:
    """Open a native file picker, falling back to a terminal prompt."""
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()  # Hide the main window
        if type == "directory":
            selected = filedialog.askdirectory(title=prompt)
        else:
            selected = filedialog.askopenfilename(
                title=prompt,
                filetypes=[("YAML files", "*.yaml"), ("CSV files", "*.csv")]
            )
        root.destroy()
        if not selected:
            raise click.ClickException("No file selected.")
   
        return Path(selected).resolve()
    except click.ClickException:
        raise
    except Exception:
        logger.warning("tkinter is not available. Falling back to terminal prompt.")
        pass

    # Fallback to terminal prompt if tkinter is not available
    selected = questionary.path(prompt).ask()
    if not selected:
        raise click.ClickException("No file selected.")
    return Path(selected).expanduser().resolve()

def main(argv: list[str] | None = None):
    args = build_parser().parse_args(argv)
    try:
        yaml_file = Path(args.yaml) or choose_file_or_directory("Select the YAML config file", "file")
        dupe_file = Path(args.file) or choose_file_or_directory("Select the CSV file to be deduped", "file")
        output_dir = Path(args.output) or choose_file_or_directory("Select the output directory", "directory")
        client_config = Utilities.load_client_config(yaml_file)
        dupe_df = Utilities.load_data_df(dupe_file)
        output_path = output_dir / f"Output_{client_config.CLIENT_NAME}_{datetime.today().date()}"
        output_path.mkdir(parents=True, exist_ok=True)
        

        main_df = Dedupe(client_cfg=client_config, df=dupe_df)
        final_df = main_df.run()
        final_df.to_csv(output_path / f"master_dedupe_{datetime.today().date()}.csv", index=False)
            

        logger.info("Dedupe complete")

    except DataLoadError as e:
        logger.exception(f"Dedupe failed during loading data: {e}")
        sys.exit(1)
    except ConfigError as e:
        logger.exception(f"Config file error: {e}")
        sys.exit(1)
    except KeyboardInterrupt as e:
        click.echo(f"{e}\nExiting...")
    except KeyError as e:
        click.echo(f"Field not found. If this is a Virtuous dedupe, ensure the 'Duplicate' prefixed fields were included in the export. {e}" )


if __name__ == "__main__":
    main()
