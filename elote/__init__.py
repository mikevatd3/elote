"""Elote - ETL package for transforming data and loading to database."""

from pathlib import Path
import json
import pandas as pd
from sqlalchemy import create_engine, table, column, select, func
from sqlalchemy.exc import ProgrammingError
import tomli
from inequalitytools import parse_to_inequality
from datetime import date


### Changes to make
# 1. check for data on the output table before loading transform
#   -> this leads to refactor where both transform and load need to know 
#      table and schema
# 2. 


def get_config():
    with open(Path.cwd() / "config.toml", "rb") as f:
        return tomli.load(f)


def get_db_engine():
    config = get_config()
    return create_engine(
        f"postgresql+psycopg://{config['db']['user']}:{config['db']['password']}"
        f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}",
        connect_args={"options": f"-csearch_path={config['app']['name']},public"},
    )


def _load_field_reference(working_dir: Path, field_reference_file: str) -> dict:
    return json.loads((working_dir / "conf" / field_reference_file).read_text())


def _transform_process(frame, field_reference):
    frame = frame.rename(columns=field_reference["renames"])

    for field in field_reference["suppressed_cols"]:
        frame[[field, f"{field}_error"]] = (
            frame[field]
            .apply(parse_to_inequality)
            .apply(lambda i: i.unwrap())
            .to_list()
        )

    return frame


def _apply_padding(frame):
    for col, padding in [("district_code", 5), ("building_code", 5)]:
        frame[col] = frame[col].str.zfill(padding)

    return frame


def _filter_datasets_on_loaded(datasets, tablename, schema):
    """
    This takes the 'datasets' dataframe and filters it to outside the
    min start_date or max end_date. This is important because we're 
    comitting to start_date and end_date on all tables if they use this 
    tool (probably okay).

    TODO: Check for gaps within the datasets too.
    """

    db = get_db_engine()
    
    t = table(tablename, column("start_date"), column("end_date"), schema=schema)
    q = select(func.min(t.c.start_date), func.max(t.c.end_date))
    
    try:
        with db.connect() as conn:
            min_start, max_end = conn.execute(q).fetchone()

    # If the table doesn't exist return everything
    except ProgrammingError:
        return datasets

    return datasets[
        (datasets["start_date"].dt.date > max_end)
        | (min_start > datasets["end_date"].dt.date)
    ]


def transform_dataset(working_dir: Path, table, schema, custom_transform=None):
    """Transform source data files into a combined CSV.

    Reads datasets.csv to find source files, applies field renames,
    processes suppressed columns, and outputs combined_years.csv.

    Args:
        working_dir: Directory containing conf/ folder with datasets.csv
                     and field_reference_*.json files.
        custom_transform: Optional function(df) -> df to apply custom
                          transformations after standard processing.
    """
    config = get_config()

    output_dir = working_dir / "output" / "combined_years.csv"
    datasets = pd.read_csv(
        working_dir / "conf" / "datasets.csv",
        parse_dates=["start_date", "end_date"],
    )

    to_load = _filter_datasets_on_loaded(datasets, table, schema)

    mode, header = "w", True
    for _, year in to_load.iterrows():
        print(f"Opening {year['source_file']}")

        field_reference = json.loads(
            (working_dir / "conf" / year["field_reference_file"]).read_text()
        )

        frame = (
            pd.read_csv(
                Path(config["vault_location"]) / year["source_file"],
                dtype=field_reference["in_types"],
                low_memory=False,
            )
            .rename(columns=field_reference["renames"])
            .pipe(lambda frame: _transform_process(frame, field_reference))
            .pipe(_apply_padding)
            .assign(start_date=year["start_date"], end_date=year["end_date"])
        )[field_reference["out_cols"]]

        if custom_transform:
            frame = custom_transform(frame)

        frame.to_csv(output_dir, mode=mode, header=header, index=False)
        mode, header = "a", False


def load_dataset(table_name: str, schema: str, working_dir: Path):
    """Load transformed data from CSV into database.

    Args:
        table_name: Name of the database table.
        working_dir: Directory containing output/combined_years.csv.
    """

    datasets = pd.read_csv(
        working_dir / "conf" / "datasets.csv",
        parse_dates=["start_date", "end_date"],
    )
    
    # Get the field reference for the last row (the out_types should all match)
    field_reference_file = datasets.iloc[-1]["field_reference_file"]
    field_reference = _load_field_reference(working_dir, field_reference_file)
    

    db_engine = get_db_engine()
    with db_engine.connect() as db:
        for i, portion in enumerate(
            pd.read_csv(
                working_dir / "output" / "combined_years.csv",
                chunksize=20_000,
                dtype=field_reference["out_types"],
                parse_dates=["start_date", "end_date"],
            ),
            start=1,
        ):
            print(f"Loading chunk {i} into database.")

            portion.to_sql(
                table_name, db, schema=schema, if_exists="append", index=False
            )
