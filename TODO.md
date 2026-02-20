# Turtle ETL system

## CLI

`turtle init` creates the directory structure in the current folder:

```
.
├── conf
│   ├── datasets.csv
│   └── field_reference_2010_2025.json
└── process.py
```

Example `datasets.csv`:

```csv
year,start_date,end_date,field_reference_file,source_file
2010,2009-07-01,2010-06-30,field_reference_2010_2025.json,DATA/Education/Mobility/Data/2010/Raw/mobility_2010.csv
2011,2010-07-01,2011-06-30,field_reference_2010_2025.json,DATA/Education/Mobility/Data/2011/Raw/mobility_2011.csv
```

Example `field_reference_<years>.json`

```json
{
    "in_types": {
        "ISDCode": "str",
        "DistrictCode": "str",
        "BuildingCode": "str"
    },
    "renames": {
        "ISDCode": "isd_code",
        "DistrictCode": "district_code",
        "BuildingCode": "building_code",
        "ReportCategory": "report_category",
        "StudentCount": "count",
        "StudentCountStable": "count_stable",
        "StudentCountMobile": "count_mobile",
        "StudentCountIncoming": "count_incoming",
        "MobilityRate": "mobility_rate"
    },
    "recodes": {},
    "suppressed_cols": [
        "count",
        "count_stable",
        "count_mobile",
        "count_incoming"
    ],
    "out_cols": [
        "isd_code",
        "district_code",
        "building_code",
        "report_category",
        "count",
        "count_stable",
        "count_mobile",
        "count_incoming",
        "mobility_rate",
        "start_date",
        "end_date"
    ],
    "out_types": {
        "isd_code": "str",
        "district_code": "str",
        "building_code": "str"
    }
}
```

And `process.py` where `transform_dataset` is basically `generic_transform` from
the current `common.py` and `load_dataset` is the same as `generic_load` from
`common.py`.

```
from pathlib import Path
from turtle import transform_dataset, load_dataset

if __name__ == "__main__":
    WORKING_DIR = Path(__file__).parent
    generic_transform(WORKING_DIR)
    generic_load("student_mobility", WORKING_DIR)
```

The thing we need to add is the user should be able to provide a custom
transformation function at some point in the pipeline.

