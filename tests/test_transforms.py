import pytest
import pandas as pd
from pathlib import Path
import json

from elote import _apply_padding, _load_field_reference


class TestApplyPadding:
    def test_pads_district_code(self):
        """Pads district_code to 5 digits."""
        df = pd.DataFrame({
            "district_code": ["1", "12", "123", "1234", "12345"],
            "building_code": ["1", "1", "1", "1", "1"]
        })

        result = _apply_padding(df)

        assert result["district_code"].tolist() == [
            "00001", "00012", "00123", "01234", "12345"
        ]

    def test_pads_building_code(self):
        """Pads building_code to 5 digits."""
        df = pd.DataFrame({
            "district_code": ["1", "1", "1"],
            "building_code": ["1", "99", "12345"]
        })

        result = _apply_padding(df)

        assert result["building_code"].tolist() == ["00001", "00099", "12345"]

    def test_preserves_other_columns(self):
        """Padding does not affect other columns."""
        df = pd.DataFrame({
            "district_code": ["1"],
            "building_code": ["1"],
            "other_col": ["unchanged"]
        })

        result = _apply_padding(df)

        assert result["other_col"].tolist() == ["unchanged"]


class TestLoadFieldReference:
    def test_loads_field_reference_json(self, temp_dir, sample_field_reference):
        """Loads field reference from JSON file."""
        conf_dir = temp_dir / "conf"
        conf_dir.mkdir()
        field_ref_file = conf_dir / "field_reference.json"
        field_ref_file.write_text(json.dumps(sample_field_reference))

        result = _load_field_reference(temp_dir, "field_reference.json")

        assert result == sample_field_reference

    def test_raises_on_missing_file(self, temp_dir):
        """Raises error when field reference file doesn't exist."""
        conf_dir = temp_dir / "conf"
        conf_dir.mkdir()

        with pytest.raises(FileNotFoundError):
            _load_field_reference(temp_dir, "nonexistent.json")


class TestTransformDataset:
    def test_custom_transform_is_applied(self, temp_dir, sample_field_reference, monkeypatch):
        """Custom transform function is applied to each year's data."""
        # This test requires more setup - mocking get_config and source files
        # Leaving as a placeholder for integration tests
        pass


class TestLoadDataset:
    def test_raises_on_missing_field_reference(self, temp_dir):
        """Raises error when no field reference file exists."""
        from elote import load_dataset

        conf_dir = temp_dir / "conf"
        conf_dir.mkdir()

        with pytest.raises(FileNotFoundError) as exc_info:
            load_dataset("test_table", temp_dir)

        assert "No field reference file found" in str(exc_info.value)
