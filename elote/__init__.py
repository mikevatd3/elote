"""Elote - ETL package for transforming data and loading to database."""

from pathlib import Path
import json
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, table, column, select, func
from sqlalchemy.exc import ProgrammingError
import tomli
from datetime import date


def get_config():
    with open(Path.cwd() / "config.toml", "rb") as f:
        return tomli.load(f)


def get_db_engine():
    config = get_config()
    return create_engine(
        f"postgresql+psycopg://{config['db']['user']}:{config['db']['password']}"
        f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}"
    )


def _load_field_reference(working_dir: Path, field_reference_file: str) -> dict:
    return json.loads((working_dir / "conf" / field_reference_file).read_text())


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
    """Yield one processed DataFrame or GeoDataFrame per source dataset.

    Reads datasets.csv to find source files not yet loaded, applies field
    renames and date columns, and yields each frame for the caller to consume.

    Args:
        working_dir: Directory containing conf/ folder with datasets.csv
                     and field_reference_*.json files.
        table: Name of the destination database table (used to check what
               date ranges are already loaded).
        schema: Database schema for the destination table.
        custom_transform: Optional function(frame, field_reference) -> frame
                          to apply custom transformations after standard
                          processing.
    """
    config = get_config()

    datasets = pd.read_csv(
        working_dir / "conf" / "datasets.csv",
        parse_dates=["start_date", "end_date"],
    )

    to_load = _filter_datasets_on_loaded(datasets, table, schema)

    for _, file_meta in to_load.iterrows():
        print(f"Opening {file_meta['source_file']}")

        field_reference = _load_field_reference(working_dir, file_meta["field_reference_file"])

        suffix = Path(file_meta["source_file"]).suffix
        if suffix in {".geojson", ".shp", ".gpkg"}:
            frame = gpd.read_file(Path(config["vault_location"]) / file_meta["source_file"])
        elif suffix == ".csv":
            frame = pd.read_csv(Path(config["vault_location"]) / file_meta["source_file"])
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

        frame = (
            frame
            .rename(columns=field_reference["renames"])
            .assign(start_date=file_meta["start_date"], end_date=file_meta["end_date"])
        )[field_reference["out_cols"]]

        if custom_transform:
            frame = custom_transform(frame, field_reference)

        yield frame


def load_dataset(frames, table_name: str, schema: str):
    """Load frames from transform_dataset into the database.

    Routes each frame to the appropriate writer: GeoDataFrames go to PostGIS
    via to_postgis(); plain DataFrames go to PostgreSQL via to_sql().

    Args:
        frames: Iterable of DataFrames or GeoDataFrames, typically the
                generator returned by transform_dataset().
        table_name: Name of the destination database table.
        schema: Database schema for the destination table.
    """
    db_engine = get_db_engine()
    for i, frame in enumerate(frames, start=1):
        print(f"Loading chunk {i} into database.")
        if isinstance(frame, gpd.GeoDataFrame):
            frame.to_postgis(table_name, db_engine, schema=schema, if_exists="append")
        else:
            with db_engine.connect() as db:
                frame.to_sql(table_name, db, schema=schema, if_exists="append", index=False)
