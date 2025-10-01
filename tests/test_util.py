#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for util.py module
"""
import json
import os

import pandas as pd

from woom import util as wutil


class TestWoomDate:
    """Test WoomDate class"""

    def test_woomdate_from_string(self):
        date = wutil.WoomDate("2025-01-15")
        assert date.year == 2025
        assert date.month == 1
        assert date.day == 15

    def test_woomdate_now(self):
        date = wutil.WoomDate("now")
        assert isinstance(date, pd.Timestamp)

    def test_woomdate_today(self):
        date = wutil.WoomDate("today")
        assert isinstance(date, pd.Timestamp)

    def test_woomdate_with_round(self):
        date = wutil.WoomDate("2025-01-15 14:35:27", round="1h")
        assert date.minute == 0
        assert date.second == 0

    def test_woomdate_format_since(self):
        date = wutil.WoomDate("2025-01-15 12:00:00")
        formatted = format(date, "hours since 2025-01-15 00:00:00")
        assert formatted == "12"

    def test_woomdate_format_days_since(self):
        date = wutil.WoomDate("2025-01-20")
        formatted = format(date, "days since 2025-01-15")
        assert formatted == "5"

    def test_woomdate_add(self):
        date = wutil.WoomDate("2025-01-15")
        new_date = date.add(days=5)
        assert new_date.day == 20


class TestCheckDir:
    """Test directory creation"""

    def test_check_dir_existing(self, tmp_path):
        filepath = tmp_path / "subdir" / "file.txt"
        (tmp_path / "subdir").mkdir()

        result = wutil.check_dir(str(filepath))
        assert os.path.dirname(result) == str(tmp_path / "subdir")

    def test_check_dir_creates(self, tmp_path):
        filepath = tmp_path / "newdir" / "file.txt"

        wutil.check_dir(str(filepath))
        assert os.path.exists(tmp_path / "newdir")

    def test_check_dir_dry_mode(self, tmp_path):
        filepath = tmp_path / "drydir" / "file.txt"

        wutil.check_dir(str(filepath), dry=True)
        assert not os.path.exists(tmp_path / "drydir")


class TestWoomJSONEncoder:
    """Test custom JSON encoder"""

    def test_encode_timestamp(self):
        data = {"date": pd.Timestamp("2025-01-15")}
        result = json.dumps(data, cls=wutil.WoomJSONEncoder)
        assert "2025-01-15" in result

    def test_encode_timedelta(self):
        data = {"duration": pd.Timedelta(days=5)}
        result = json.dumps(data, cls=wutil.WoomJSONEncoder)
        assert result is not None


class TestParams2EnvVars:
    """Test parameter to environment variable conversion"""

    def test_params2env_vars_simple(self):
        params = {"key1": "value1", "key2": "value2"}
        result = wutil.params2env_vars(params)
        assert result["WOOM_KEY1"] == "value1"
        assert result["WOOM_KEY2"] == "value2"

    def test_params2env_vars_timestamp(self):
        params = {"date": pd.Timestamp("2025-01-15")}
        result = wutil.params2env_vars(params)
        assert "2025-01-15" in result["WOOM_DATE"]

    def test_params2env_vars_none(self):
        params = {"key": None}
        result = wutil.params2env_vars(params)
        assert result["WOOM_KEY"] == ""

    def test_params2env_vars_bool(self):
        params = {"flag": True}
        result = wutil.params2env_vars(params)
        assert result["WOOM_FLAG"] == "1"

    def test_params2env_vars_extra(self):
        result = wutil.params2env_vars(key1="value1", key2="value2")
        assert result["WOOM_KEY1"] == "value1"
        assert result["WOOM_KEY2"] == "value2"


class TestPages2Ints:
    """Test page selection to integers conversion"""

    def test_pages2ints_simple(self):
        pages = [1, 3, 5]
        result = wutil.pages2ints(pages, 10)
        assert result == [1, 3, 5]

    def test_pages2ints_with_slice(self):
        pages = [slice(2, 5)]
        result = wutil.pages2ints(pages, 10)
        assert result == [3, 4, 5]

    def test_pages2ints_mixed(self):
        pages = [1, slice(3, 5), 8]
        result = wutil.pages2ints(pages, 10)
        assert result == [1, 4, 5, 8]

    def test_pages2ints_open_slice(self):
        pages = [slice(7, None)]
        result = wutil.pages2ints(pages, 10)
        assert result == [8, 9, 10]
