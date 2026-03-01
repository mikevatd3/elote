"""CLI for elote package."""

import click
from pathlib import Path

DATASETS_CSV_TEMPLATE = """year,start_date,end_date,field_reference_file,source_file,source_type
2010,2009-07-01,2010-06-30,field_reference.json,DATA/path/to/2010/data.csv,
2011,2010-07-01,2011-06-30,field_reference.json,DATA/path/to/2011/data.csv,
"""

FIELD_REFERENCE_TEMPLATE = """{
    "in_types": {
        "DistrictCode": "str",
        "BuildingCode": "str"
    },
    "renames": {
        "DistrictCode": "district_code",
        "BuildingCode": "building_code"
    },
    "recodes": {},
    "suppressed_cols": [],
    "out_cols": [
        "district_code",
        "building_code",
        "start_date",
        "end_date"
    ],
    "out_types": {
        "district_code": "str",
        "building_code": "str"
    }
}
"""

PROCESS_PY_TEMPLATE = '''from pathlib import Path
from elote import transform_dataset, load_dataset

TABLE_NAME = None
SCHEMA = None

"""
Write a custom transformation function here and provide it to the 
'transform_dataset' function with kwarg 'custom_transform.' This function 
should take a dataframe and return a dataframe. It should only operate to 
clean rows and not do any aggregations. Elote isn't meant for complex 
aggregations (ELT vs ETL).
"""


if __name__ == "__main__":
    WORKING_DIR = Path(__file__).parent

    frames = transform_dataset(WORKING_DIR, table=TABLE_NAME, schema=SCHEMA)
    load_dataset(frames, table_name=TABLE_NAME, schema=SCHEMA)
'''


@click.group()
def cli():
    """Elote - ETL for transforming data and loading to database."""
    pass


@cli.command()
def init():
    """Initialize a new elote dataset project in the current directory."""
    cwd = Path.cwd()

    # Create conf directory
    conf_dir = cwd / "conf"
    conf_dir.mkdir(exist_ok=True)

    # Create output directory
    output_dir = cwd / "output"
    output_dir.mkdir(exist_ok=True)

    # Create datasets.csv
    datasets_csv = conf_dir / "datasets.csv"
    if not datasets_csv.exists():
        datasets_csv.write_text(DATASETS_CSV_TEMPLATE)
        click.echo(f"Created {datasets_csv}")
    else:
        click.echo(f"Skipped {datasets_csv} (already exists)")

    # Create field_reference.json
    field_ref = conf_dir / "field_reference.json"
    if not field_ref.exists():
        field_ref.write_text(FIELD_REFERENCE_TEMPLATE)
        click.echo(f"Created {field_ref}")
    else:
        click.echo(f"Skipped {field_ref} (already exists)")

    # Create process.py
    process_py = cwd / "process.py"
    if not process_py.exists():
        process_py.write_text(PROCESS_PY_TEMPLATE)
        click.echo(f"Created {process_py}")
    else:
        click.echo(f"Skipped {process_py} (already exists)")

    click.echo("\nInitialized elote project. Next steps:")
    click.echo("  1. Edit conf/datasets.csv with your source files")
    click.echo("  2. Edit conf/field_reference.json with your field mappings")
    click.echo("  3. Run: python process.py")


if __name__ == "__main__":
    cli()
