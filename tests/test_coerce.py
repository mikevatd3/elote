import pytest
import pandas as pd

from elote.coerce import coerce_bool_series


class TestCoerceBoolSeries:
    def _coerce(self, values):
        return coerce_bool_series(pd.Series(values))

    def test_numeric_one_and_zero(self):
        result = self._coerce([1, 0])
        assert result[0] == True
        assert result[1] == False

    def test_string_one_and_zero(self):
        result = self._coerce(["1", "0"])
        assert result[0] == True
        assert result[1] == False

    def test_float_one_and_zero(self):
        result = self._coerce([1.0, 0.0])
        assert result[0] == True
        assert result[1] == False

    def test_yes_no_case_insensitive(self):
        result = self._coerce(["Yes", "NO", "yes", "no", "YES"])
        assert list(result) == [True, False, True, False, True]

    def test_true_false_strings_case_insensitive(self):
        result = self._coerce(["True", "False", "TRUE", "FALSE", "true", "false"])
        assert list(result) == [True, False, True, False, True, False]

    def test_whitespace_stripped(self):
        result = self._coerce(["  yes  ", "  no  "])
        assert result[0] == True
        assert result[1] == False

    def test_none_becomes_na(self):
        result = self._coerce([None])
        assert pd.isna(result[0])

    def test_nan_becomes_na(self):
        result = self._coerce([float("nan")])
        assert pd.isna(result[0])

    def test_returns_nullable_boolean_dtype(self):
        result = self._coerce(["yes", "no"])
        assert result.dtype == pd.BooleanDtype()

    def test_raises_on_unrecognised_value(self):
        with pytest.raises(ValueError, match="Cannot convert"):
            self._coerce(["maybe"])

    def test_raises_on_arbitrary_integer(self):
        with pytest.raises(ValueError, match="Cannot convert"):
            self._coerce([2])

    def test_error_message_includes_bad_value(self):
        with pytest.raises(ValueError, match="'invalid'"):
            self._coerce(["invalid"])
