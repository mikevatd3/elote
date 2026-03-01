"""Type coercion utilities for elote transforms."""

import pandas as pd


_BOOL_TRUE = frozenset({1, "1", "yes", "true"})
_BOOL_FALSE = frozenset({0, "0", "no", "false"})


def _bool_value(val):
    """Convert a single value to bool, raising on unrecognised input."""
    if pd.isna(val):
        return pd.NA
    key = val.strip().lower() if isinstance(val, str) else val
    if key in _BOOL_TRUE:
        return True
    if key in _BOOL_FALSE:
        return False
    raise ValueError(
        f"Cannot convert {val!r} to bool. "
        f"Expected one of: 1, 0, 'yes', 'no', 'true', 'false'."
    )


def coerce_bool_series(series: pd.Series) -> pd.Series:
    """Convert a Series to nullable boolean, accepting 1/0, yes/no, true/false.

    Raises ValueError on the first value that doesn't match any known pattern.
    """
    return series.map(_bool_value).astype("boolean")
