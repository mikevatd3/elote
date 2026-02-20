import pytest
from pathlib import Path
import tempfile
import shutil


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    dirpath = tempfile.mkdtemp()
    yield Path(dirpath)
    shutil.rmtree(dirpath)


@pytest.fixture
def sample_field_reference():
    """Sample field reference configuration."""
    return {
        "in_types": {
            "DistrictCode": "str",
            "BuildingCode": "str"
        },
        "renames": {
            "DistrictCode": "district_code",
            "BuildingCode": "building_code",
            "Value": "value"
        },
        "recodes": {},
        "suppressed_cols": [],
        "out_cols": [
            "district_code",
            "building_code",
            "value",
            "start_date",
            "end_date"
        ],
        "out_types": {
            "district_code": "str",
            "building_code": "str"
        }
    }
