import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path
from unittest.mock import MagicMock, patch
import json

from elote import transform_dataset, load_dataset


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

    def test_raises_on_unsupported_file_type(self, temp_dir, sample_field_reference, monkeypatch):
        """Raises ValueError for unsupported file extensions."""
        self._setup_working_dir(temp_dir, sample_field_reference, "data/file.xlsx")

        monkeypatch.setattr("elote.get_config", lambda: {"vault_location": "/vault"})
        monkeypatch.setattr("elote._filter_datasets_on_loaded", lambda datasets, t, s: datasets)

        with pytest.raises(ValueError, match="Unsupported file type"):
            list(transform_dataset(temp_dir, table="my_table", schema="public"))

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


class TestLoadDataset:
    def test_calls_to_sql_for_dataframe(self, monkeypatch):
        """Calls to_sql (not to_postgis) for a plain DataFrame."""
        frame = pd.DataFrame({"a": [1, 2]})
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        monkeypatch.setattr("elote.get_db_engine", lambda: mock_engine)

        frame_mock = MagicMock(spec=pd.DataFrame)
        # Ensure isinstance check treats this as DataFrame, not GeoDataFrame
        frames = [frame]

        with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
            load_dataset(iter(frames), table_name="my_table", schema="public")
            mock_to_sql.assert_called_once()
            call_kwargs = mock_to_sql.call_args
            assert call_kwargs.kwargs.get("if_exists") == "append"
            assert call_kwargs.kwargs.get("schema") == "public"

    def test_calls_to_postgis_for_geodataframe(self, monkeypatch):
        """Calls to_postgis (not to_sql) for a GeoDataFrame."""
        gdf = gpd.GeoDataFrame({"a": [1], "geometry": [Point(0, 0)]})
        mock_engine = MagicMock()

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
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        monkeypatch.setattr("elote.get_db_engine", lambda: mock_engine)

        with (
            patch.object(pd.DataFrame, "to_sql") as mock_to_sql,
            patch.object(gpd.GeoDataFrame, "to_postgis") as mock_to_postgis,
        ):
            load_dataset(iter([df, gdf]), table_name="my_table", schema="public")
            assert mock_to_sql.call_count == 1
            assert mock_to_postgis.call_count == 1
