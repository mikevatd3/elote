import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path
from unittest.mock import MagicMock, patch
import json

from sqlalchemy.exc import OperationalError, ProgrammingError

from elote import transform_dataset, load_dataset, _filter_datasets_on_loaded


class TestTransformDataset:
    def _setup_working_dir(self, temp_dir, sample_field_reference, source_file):
        conf_dir = temp_dir / "conf"
        conf_dir.mkdir()
        (conf_dir / "field_reference.json").write_text(json.dumps(sample_field_reference))
        (conf_dir / "datasets.csv").write_text(
            "year,start_date,end_date,field_reference_file,source_file\n"
            f"2010,2009-07-01,2010-06-30,field_reference.json,{source_file}\n"
        )

    def test_yields_dataframe_for_csv_source(self, temp_dir, sample_field_reference, monkeypatch):
        """Yields a plain DataFrame when the source file is a CSV."""
        self._setup_working_dir(temp_dir, sample_field_reference, "data/file.csv")

        source_df = pd.DataFrame({
            "DistrictCode": ["001"],
            "BuildingCode": ["002"],
            "Value": [42],
        })

        _real_read_csv = pd.read_csv

        def _mock_read_csv(path, **kw):
            if str(path).endswith("datasets.csv"):
                return _real_read_csv(path, **kw)
            return source_df

        monkeypatch.setattr("elote.get_config", lambda: {"vault_location": "/vault"})
        monkeypatch.setattr("elote._filter_datasets_on_loaded", lambda datasets, t, s: datasets)
        monkeypatch.setattr("pandas.read_csv", _mock_read_csv)

        frames = list(transform_dataset(temp_dir, table="my_table", schema="public"))

        assert len(frames) == 1
        assert type(frames[0]) is pd.DataFrame
        assert not isinstance(frames[0], gpd.GeoDataFrame)

    def test_yields_geodataframe_for_geo_source(self, temp_dir, sample_field_reference, monkeypatch):
        """Yields a GeoDataFrame when the source file is a GeoJSON."""
        geo_field_reference = {
            **sample_field_reference,
            "out_cols": sample_field_reference["out_cols"] + ["geometry"],
        }
        self._setup_working_dir(temp_dir, geo_field_reference, "data/file.geojson")

        source_gdf = gpd.GeoDataFrame({
            "DistrictCode": ["001"],
            "BuildingCode": ["002"],
            "Value": [42],
            "geometry": [Point(0, 0)],
        })

        monkeypatch.setattr("elote.get_config", lambda: {"vault_location": "/vault"})
        monkeypatch.setattr("elote._filter_datasets_on_loaded", lambda datasets, t, s: datasets)
        monkeypatch.setattr("geopandas.read_file", lambda path, **kw: source_gdf)

        frames = list(transform_dataset(temp_dir, table="my_table", schema="public"))

        assert len(frames) == 1
        assert isinstance(frames[0], gpd.GeoDataFrame)

    def test_custom_transform_receives_frame_and_field_reference(
        self, temp_dir, sample_field_reference, monkeypatch
    ):
        """custom_transform is called with (frame, field_reference)."""
        self._setup_working_dir(temp_dir, sample_field_reference, "data/file.csv")

        source_df = pd.DataFrame({
            "DistrictCode": ["001"],
            "BuildingCode": ["002"],
            "Value": [42],
        })

        _real_read_csv = pd.read_csv

        def _mock_read_csv(path, **kw):
            if str(path).endswith("datasets.csv"):
                return _real_read_csv(path, **kw)
            return source_df

        monkeypatch.setattr("elote.get_config", lambda: {"vault_location": "/vault"})
        monkeypatch.setattr("elote._filter_datasets_on_loaded", lambda datasets, t, s: datasets)
        monkeypatch.setattr("pandas.read_csv", _mock_read_csv)

        received = {}

        def custom_transform(frame, field_reference):
            received["field_reference"] = field_reference
            return frame

        list(transform_dataset(
            temp_dir, table="my_table", schema="public", custom_transform=custom_transform
        ))

        assert received["field_reference"] == sample_field_reference

    def test_blank_string_becomes_na_when_casting_to_int(self, temp_dir, sample_field_reference, monkeypatch):
        """Blank strings in an int column become pd.NA rather than raising."""
        ref = {
            **sample_field_reference,
            "out_types": {"value": "int"},
        }
        self._setup_working_dir(temp_dir, ref, "data/file.csv")

        source_df = pd.DataFrame({
            "DistrictCode": ["001", "002"],
            "BuildingCode": ["A", "B"],
            "Value": ["42", ""],
        })

        _real_read_csv = pd.read_csv

        def _mock_read_csv(path, **kw):
            if str(path).endswith("datasets.csv"):
                return _real_read_csv(path, **kw)
            return source_df

        monkeypatch.setattr("elote.get_config", lambda: {"vault_location": "/vault"})
        monkeypatch.setattr("elote._filter_datasets_on_loaded", lambda datasets, t, s: datasets)
        monkeypatch.setattr("pandas.read_csv", _mock_read_csv)

        frames = list(transform_dataset(temp_dir, table="my_table", schema="public"))

        col = frames[0]["value"]
        assert col.dtype.name == "Int64"
        assert col[0] == 42
        assert pd.isna(col[1])

    def test_out_types_cast_columns(self, temp_dir, sample_field_reference, monkeypatch):
        """out_types: {"value": "int"} produces an integer column, not float."""
        ref = {
            **sample_field_reference,
            "out_types": {"value": "int"},
        }
        self._setup_working_dir(temp_dir, ref, "data/file.csv")

        # CSV with a numeric column that pandas would read as float by default
        source_df = pd.DataFrame({
            "DistrictCode": ["001"],
            "BuildingCode": ["002"],
            "Value": [42.0],
        })

        _real_read_csv = pd.read_csv

        def _mock_read_csv(path, **kw):
            if str(path).endswith("datasets.csv"):
                return _real_read_csv(path, **kw)
            return source_df

        monkeypatch.setattr("elote.get_config", lambda: {"vault_location": "/vault"})
        monkeypatch.setattr("elote._filter_datasets_on_loaded", lambda datasets, t, s: datasets)
        monkeypatch.setattr("pandas.read_csv", _mock_read_csv)

        frames = list(transform_dataset(temp_dir, table="my_table", schema="public"))

        assert frames[0]["value"].dtype.name == "Int64"

    def test_date_cols_included_when_absent_from_out_cols(self, temp_dir, sample_field_reference, monkeypatch):
        """start_date and end_date are always in the output even if omitted from out_cols."""
        ref_without_dates = {
            **sample_field_reference,
            "out_cols": ["district_code", "building_code", "value"],
        }
        self._setup_working_dir(temp_dir, ref_without_dates, "data/file.csv")

        source_df = pd.DataFrame({
            "DistrictCode": ["001"],
            "BuildingCode": ["002"],
            "Value": [42],
        })

        _real_read_csv = pd.read_csv

        def _mock_read_csv(path, **kw):
            if str(path).endswith("datasets.csv"):
                return _real_read_csv(path, **kw)
            return source_df

        monkeypatch.setattr("elote.get_config", lambda: {"vault_location": "/vault"})
        monkeypatch.setattr("elote._filter_datasets_on_loaded", lambda datasets, t, s: datasets)
        monkeypatch.setattr("pandas.read_csv", _mock_read_csv)

        frames = list(transform_dataset(temp_dir, table="my_table", schema="public"))

        assert len(frames) == 1
        assert "start_date" in frames[0].columns
        assert "end_date" in frames[0].columns

    def test_raises_on_unsupported_file_type(self, temp_dir, sample_field_reference, monkeypatch):
        """Raises ValueError for unsupported file extensions."""
        self._setup_working_dir(temp_dir, sample_field_reference, "data/file.xlsx")

        monkeypatch.setattr("elote.get_config", lambda: {"vault_location": "/vault"})
        monkeypatch.setattr("elote._filter_datasets_on_loaded", lambda datasets, t, s: datasets)

        with pytest.raises(ValueError, match="Unsupported file type"):
            list(transform_dataset(temp_dir, table="my_table", schema="public"))

    def _setup_working_dir_with_source_type(self, temp_dir, sample_field_reference, source_file, source_type):
        conf_dir = temp_dir / "conf"
        conf_dir.mkdir(exist_ok=True)
        (conf_dir / "field_reference.json").write_text(json.dumps(sample_field_reference))
        (conf_dir / "datasets.csv").write_text(
            "year,start_date,end_date,field_reference_file,source_file,source_type\n"
            f"2010,2009-07-01,2010-06-30,field_reference.json,{source_file},{source_type}\n"
        )

    def test_reads_from_db_table(self, temp_dir, sample_field_reference, monkeypatch):
        """Reads from DB when source_type=db and source_file contains schema.table."""
        self._setup_working_dir_with_source_type(
            temp_dir, sample_field_reference, "public.raw_mobility", "db"
        )

        source_df = pd.DataFrame({
            "DistrictCode": ["001"],
            "BuildingCode": ["002"],
            "Value": [42],
        })

        mock_engine = MagicMock()
        monkeypatch.setattr("elote.get_config", lambda: {})
        monkeypatch.setattr("elote._filter_datasets_on_loaded", lambda datasets, t, s: datasets)

        with patch("pandas.read_sql_table", return_value=source_df) as mock_read_sql, \
             patch("elote.get_db_engine", return_value=mock_engine):
            frames = list(transform_dataset(temp_dir, table="my_table", schema="public"))

        assert len(frames) == 1
        mock_read_sql.assert_called_once()
        call_kwargs = mock_read_sql.call_args
        assert call_kwargs.args[0] == "raw_mobility"
        assert call_kwargs.kwargs.get("schema") == "public"

    def test_reads_from_db_table_without_schema(self, temp_dir, sample_field_reference, monkeypatch):
        """Reads from DB with schema=None when source_file has no dot prefix."""
        self._setup_working_dir_with_source_type(
            temp_dir, sample_field_reference, "raw_mobility", "db"
        )

        source_df = pd.DataFrame({
            "DistrictCode": ["001"],
            "BuildingCode": ["002"],
            "Value": [42],
        })

        mock_engine = MagicMock()
        monkeypatch.setattr("elote.get_config", lambda: {})
        monkeypatch.setattr("elote._filter_datasets_on_loaded", lambda datasets, t, s: datasets)

        with patch("pandas.read_sql_table", return_value=source_df) as mock_read_sql, \
             patch("elote.get_db_engine", return_value=mock_engine):
            frames = list(transform_dataset(temp_dir, table="my_table", schema="public"))

        assert len(frames) == 1
        mock_read_sql.assert_called_once()
        call_kwargs = mock_read_sql.call_args
        assert call_kwargs.args[0] == "raw_mobility"
        assert call_kwargs.kwargs.get("schema") is None

    def test_yields_nothing_when_all_dates_loaded(self, temp_dir, sample_field_reference, monkeypatch):
        """Yields no frames when _filter_datasets_on_loaded returns an empty DataFrame."""
        self._setup_working_dir(temp_dir, sample_field_reference, "data/file.csv")

        monkeypatch.setattr("elote.get_config", lambda: {"vault_location": "/vault"})
        monkeypatch.setattr(
            "elote._filter_datasets_on_loaded",
            lambda datasets, t, s: datasets.iloc[0:0],  # empty slice
        )

        frames = list(transform_dataset(temp_dir, table="my_table", schema="public"))

        assert frames == []


class TestFilterDatasetsOnLoaded:
    def _make_datasets(self):
        return pd.DataFrame({
            "start_date": pd.to_datetime(["2009-07-01"]),
            "end_date": pd.to_datetime(["2010-06-30"]),
        })

    def test_returns_all_datasets_when_table_missing_postgres(self, monkeypatch):
        """ProgrammingError (PostgreSQL missing table) returns all datasets."""
        mock_engine = MagicMock()
        mock_engine.dialect.name = 'postgresql'
        mock_engine.connect.return_value.__enter__.side_effect = ProgrammingError(
            "relation does not exist", {}, None
        )
        monkeypatch.setattr("elote.get_db_engine", lambda: mock_engine)

        datasets = self._make_datasets()
        result = _filter_datasets_on_loaded(datasets, "my_table", "public")
        assert len(result) == len(datasets)

    def test_returns_all_datasets_when_table_missing_sqlite(self, monkeypatch):
        """OperationalError (SQLite missing table) returns all datasets."""
        mock_engine = MagicMock()
        mock_engine.dialect.name = 'sqlite'
        mock_engine.connect.return_value.__enter__.side_effect = OperationalError(
            "no such table: my_table", {}, None
        )
        monkeypatch.setattr("elote.get_db_engine", lambda: mock_engine)

        datasets = self._make_datasets()
        result = _filter_datasets_on_loaded(datasets, "my_table", None)
        assert len(result) == len(datasets)

    def test_filters_correctly_when_sqlite_returns_iso8601_strings(self, monkeypatch):
        """SQLite returns date strings; comparison still works correctly."""
        mock_engine = MagicMock()
        mock_engine.dialect.name = 'sqlite'
        mock_conn = MagicMock()
        # SQLite returns ISO8601 strings, not date objects
        mock_conn.execute.return_value.fetchone.return_value = ("2009-07-01", "2010-06-30")
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr("elote.get_db_engine", lambda: mock_engine)

        datasets = pd.DataFrame({
            "start_date": pd.to_datetime(["2008-07-01", "2010-07-01"]),
            "end_date":   pd.to_datetime(["2009-06-30", "2011-06-30"]),
        })
        result = _filter_datasets_on_loaded(datasets, "my_table", None)
        # 2008 row is before min, 2010 row is after max — both outside loaded range
        assert len(result) == 2


def _pg_engine():
    mock_engine = MagicMock()
    mock_engine.dialect.name = 'postgresql'
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return mock_engine


def _sqlite_engine():
    mock_engine = MagicMock()
    mock_engine.dialect.name = 'sqlite'
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return mock_engine


class TestLoadDataset:
    def test_calls_to_sql_for_dataframe(self, monkeypatch):
        """Calls to_sql (not to_postgis) for a plain DataFrame."""
        frame = pd.DataFrame({"a": [1, 2]})
        mock_engine = _pg_engine()

        monkeypatch.setattr("elote.get_db_engine", lambda: mock_engine)

        with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
            load_dataset(iter([frame]), table_name="my_table", schema="public")
            mock_to_sql.assert_called_once()
            call_kwargs = mock_to_sql.call_args
            assert call_kwargs.kwargs.get("if_exists") == "append"
            assert call_kwargs.kwargs.get("schema") == "public"

    def test_calls_to_postgis_for_geodataframe(self, monkeypatch):
        """Calls to_postgis (not to_sql) for a GeoDataFrame on postgres."""
        gdf = gpd.GeoDataFrame({"a": [1], "geometry": [Point(0, 0)]})
        mock_engine = _pg_engine()

        monkeypatch.setattr("elote.get_db_engine", lambda: mock_engine)

        with patch.object(gpd.GeoDataFrame, "to_postgis") as mock_to_postgis:
            load_dataset(iter([gdf]), table_name="my_table", schema="public")
            mock_to_postgis.assert_called_once()
            call_kwargs = mock_to_postgis.call_args
            assert call_kwargs.kwargs.get("if_exists") == "append"
            assert call_kwargs.kwargs.get("schema") == "public"
            # Engine is passed directly (not a connection)
            assert call_kwargs.args[1] is mock_engine

    def test_routes_mixed_frames_correctly(self, monkeypatch):
        """Dispatches each frame type independently in a mixed sequence."""
        df = pd.DataFrame({"a": [1]})
        gdf = gpd.GeoDataFrame({"a": [1], "geometry": [Point(0, 0)]})
        mock_engine = _pg_engine()

        monkeypatch.setattr("elote.get_db_engine", lambda: mock_engine)

        with (
            patch.object(pd.DataFrame, "to_sql") as mock_to_sql,
            patch.object(gpd.GeoDataFrame, "to_postgis") as mock_to_postgis,
        ):
            load_dataset(iter([df, gdf]), table_name="my_table", schema="public")
            assert mock_to_sql.call_count == 1
            assert mock_to_postgis.call_count == 1

    def test_geodataframe_written_via_to_sql_on_sqlite(self, monkeypatch):
        """GeoDataFrame is flattened to WKB hex and written via to_sql on SQLite."""
        gdf = gpd.GeoDataFrame({"a": [1], "geometry": [Point(0, 0)]})
        mock_engine = _sqlite_engine()

        monkeypatch.setattr("elote.get_db_engine", lambda: mock_engine)

        with (
            patch.object(pd.DataFrame, "to_sql") as mock_to_sql,
            patch.object(gpd.GeoDataFrame, "to_postgis") as mock_to_postgis,
        ):
            load_dataset(iter([gdf]), table_name="my_table", schema="public")
            mock_to_postgis.assert_not_called()
            mock_to_sql.assert_called_once()
            call_kwargs = mock_to_sql.call_args
            assert call_kwargs.kwargs.get("if_exists") == "append"
            # schema is ignored for SQLite
            assert call_kwargs.kwargs.get("schema") is None
