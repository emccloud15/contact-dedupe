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
    parser.add_argument("--dir", dest="input_dir", type=CleanPath(), help="Directory containing one YAML and one CSV")
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
        if args.input_dir:
            yaml_file, dupe_file = Utilities.load_data_from_dir(Path(args.input_dir))
        else:
            print("This is the new version")
            yaml_file = Path(args.yaml) if args.yaml else choose_file_or_directory("Select the YAML config file", "file")
            dupe_file = Path(args.file) if args.file else choose_file_or_directory("Select the CSV file to be deduped", "file")
        output_dir = Path(args.output) if args.output else choose_file_or_directory("Select the output directory", "directory")
        client_config = Utilities.load_client_config(yaml_file)
        if client_config.needs_weight_balance():
            auto_balance = questionary.confirm(
                "Some contact columns have no weight. Auto-balance them?",
                default=True,
            ).ask()
            if not auto_balance:
                raise ConfigError("Weight configuration was not auto-balanced; exiting.")
            client_config.auto_balance_weights()
        dupe_df = Utilities.load_data_df(dupe_file)
        output_path = output_dir / f"Output_{client_config.CLIENT_NAME}_{datetime.today().date()}"
        main_df = Dedupe(client_cfg=client_config, df=dupe_df)
        main_df.run()
        artifacts = main_df.write_outputs(str(output_path))
        click.echo("Dedupe complete")
        group_count = len(main_df.grouping.groups) if main_df.grouping is not None else 0
        click.echo(f"Candidates: {len(main_df.candidate_pairs)}; groups: {group_count}")
        for artifact in artifacts.values():
            click.echo(str(artifact))

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


if __name__ == "__main__":  # pragma: no cover - exercised through the console entry point
    main()
