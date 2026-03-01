"""Elote - ETL package for transforming data and loading to database."""

from pathlib import Path
import json
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, table, column, select, func
from sqlalchemy.exc import ProgrammingError, OperationalError
import tomli
from datetime import date, datetime
from elote.coerce import coerce_bool_series


def get_config():
    with open(Path.cwd() / "config.toml", "rb") as f:
        return tomli.load(f)


def get_db_engine():
    config = get_config()
    db = config['db']
    db_type = db.get('type', 'postgresql')

    if db_type == 'postgresql':
        return create_engine(
            f"postgresql+psycopg://{db['user']}:{db['password']}"
            f"@{db['host']}:{db['port']}/{db['name']}"
        )
    elif db_type == 'sqlite':
        return create_engine(f"sqlite:///{db['path']}")
    else:
        raise ValueError(f"Unsupported db type: {db_type!r}. Use 'postgresql' or 'sqlite'.")


_TYPE_MAP = {
    "int": "Int64",
    "float": float,
    "str": str,
}


def _resolve_types(type_dict: dict) -> dict:
    return {col: _TYPE_MAP.get(t, t) for col, t in type_dict.items()}



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
    effective_schema = schema if db.dialect.name == 'postgresql' else None
    t = table(tablename, column("start_date"), column("end_date"), schema=effective_schema)
    q = select(func.min(t.c.start_date), func.max(t.c.end_date))
    
    try:
        with db.connect() as conn:
            min_start, max_end = conn.execute(q).fetchone()

    # If the table doesn't exist return everything
    # ProgrammingError: PostgreSQL; OperationalError: SQLite
    except (ProgrammingError, OperationalError):
        return datasets

    # SQLite returns ISO8601 strings; PostgreSQL returns date objects. Normalise.
    if isinstance(min_start, str):
        min_start = datetime.fromisoformat(min_start).date()
    if isinstance(max_end, str):
        max_end = datetime.fromisoformat(max_end).date()

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

    has_source_type = "source_type" in to_load.columns

    for _, file_meta in to_load.iterrows():
        print(f"Opening {file_meta['source_file']}")

        field_reference = _load_field_reference(working_dir, file_meta["field_reference_file"])

        source_type = file_meta["source_type"] if has_source_type and pd.notna(file_meta.get("source_type")) else "file"

        if source_type == "db":
            table_spec = file_meta["source_file"]
            if "." in table_spec:
                src_schema, src_table = table_spec.split(".", 1)
            else:
                src_schema, src_table = None, table_spec
            frame = pd.read_sql_table(src_table, get_db_engine(), schema=src_schema or None)
        else:
            suffix = Path(file_meta["source_file"]).suffix
            in_types = _resolve_types(field_reference.get("in_types", {}))

            if suffix in {".geojson", ".shp", ".gpkg"}:
                frame = gpd.read_file(Path(config["vault_location"]) / file_meta["source_file"])
            elif suffix == ".csv":
                frame = pd.read_csv(
                    Path(config["vault_location"]) / file_meta["source_file"],
                    dtype=in_types,
                    skipinitialspace=True,
                )
            else:
                raise ValueError(f"Unsupported file type: {suffix}")

        out_cols = field_reference["out_cols"]
        date_cols = [c for c in ("start_date", "end_date") if c not in out_cols]
        frame = (
            frame
            .rename(columns=field_reference["renames"])
            .assign(start_date=file_meta["start_date"], end_date=file_meta["end_date"])
        )[out_cols + date_cols]

        raw_out_types = field_reference.get("out_types", {})
        bool_cols = [col for col, t in raw_out_types.items() if t == "bool"]
        other_types = _resolve_types({col: t for col, t in raw_out_types.items() if t != "bool"})

        if other_types:
            for col, dtype in other_types.items():
                if col in frame.columns and dtype in ("Int64", float):
                    frame[col] = pd.to_numeric(frame[col], errors='coerce')
            frame = frame.astype(other_types)
        for col in bool_cols:
            frame[col] = coerce_bool_series(frame[col])

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
    is_postgres = db_engine.dialect.name == 'postgresql'
    effective_schema = schema if is_postgres else None

    for i, frame in enumerate(frames, start=1):
        print(f"Loading chunk {i} into database.")
        if isinstance(frame, gpd.GeoDataFrame) and is_postgres:
            frame.to_postgis(table_name, db_engine, schema=schema, if_exists="append")
        else:
            if isinstance(frame, gpd.GeoDataFrame):
                flat = pd.DataFrame(frame)
                flat['geometry'] = flat['geometry'].apply(
                    lambda g: g.wkb_hex if g is not None else None
                )
                frame = flat
            with db_engine.connect() as db:
                frame.to_sql(table_name, db, schema=effective_schema, if_exists="append", index=False)
