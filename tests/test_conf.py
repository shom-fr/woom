#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for conf.py module
"""
import pathlib

import configobj
import pandas as pd
import pytest

from woom import conf as wconf


class TestValidators:
    """Test validator functions"""

    def test_is_path_valid(self):
        result = wconf.is_path("/tmp/test")
        assert isinstance(result, pathlib.Path)
        assert str(result) == "/tmp/test"

    def test_is_path_none(self):
        result = wconf.is_path(None)
        assert result is None

    def test_is_datetime_valid(self):
        result = wconf.is_datetime("2025-01-01")
        assert isinstance(result, pd.Timestamp)
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 1

    def test_is_datetime_with_round(self):
        result = wconf.is_datetime("2025-01-01 12:34:56", round="1h")
        assert result.minute == 0
        assert result.second == 0

    def test_is_datetime_none(self):
        result = wconf.is_datetime(None)
        assert result is None

    def test_is_timedelta_valid(self):
        result = wconf.is_timedelta("1 day")
        assert isinstance(result, pd.Timedelta)
        assert result.days == 1

    def test_is_timedelta_none(self):
        result = wconf.is_timedelta(None)
        assert result is None

    def test_is_pages_single(self):
        result = wconf.is_pages("4")
        assert result == [4]

    def test_is_pages_multiple(self):
        result = wconf.is_pages("4,5,7")
        assert result == [4, 5, 7]

    def test_is_pages_range(self):
        result = wconf.is_pages("3-5")
        assert isinstance(result[0], slice)
        assert result[0].start == 2
        assert result[0].stop == 5

    def test_is_pages_open_range(self):
        result = wconf.is_pages("7-")
        assert isinstance(result[0], slice)
        assert result[0].start == 6
        assert result[0].stop is None


class TestGetValidator:
    """Test validator instance creation"""

    def test_get_validator(self):
        validator = wconf.get_validator()
        assert validator is not None
        assert "path" in validator.functions
        assert "datetime" in validator.functions
        assert "timedelta" in validator.functions
        assert "pages" in validator.functions


class TestConfigLoading:
    """Test configuration loading"""

    def test_get_cfgspecs_single(self, tmp_path):
        cfg_file = tmp_path / "test.ini"
        cfg_file.write_text("[section]\nkey=string(default=value)")

        cfgspecs = wconf.get_cfgspecs(str(cfg_file))
        assert isinstance(cfgspecs, configobj.ConfigObj)
        assert "section" in cfgspecs

    def test_get_cfgspecs_multiple(self, tmp_path):
        cfg1 = tmp_path / "test1.ini"
        cfg1.write_text("[section1]\nkey1=string(default=value1)")
        cfg2 = tmp_path / "test2.ini"
        cfg2.write_text("[section2]\nkey2=string(default=value2)")

        cfgspecs = wconf.get_cfgspecs([str(cfg1), str(cfg2)])
        assert "section1" in cfgspecs
        assert "section2" in cfgspecs

    def test_load_cfg_valid(self, tmp_path):
        spec_file = tmp_path / "spec.ini"
        spec_file.write_text("[section]\nkey=string(default=test)")

        cfg_file = tmp_path / "config.cfg"
        cfg_file.write_text("[section]\nkey=myvalue")

        cfg = wconf.load_cfg(str(cfg_file), str(spec_file))
        assert cfg["section"]["key"] == "myvalue"

    def test_load_cfg_invalid(self, tmp_path):
        spec_file = tmp_path / "spec.ini"
        spec_file.write_text("[section]\nkey=integer")

        cfg_file = tmp_path / "config.cfg"
        cfg_file.write_text("[section]\nkey=notanumber")

        with pytest.raises(wconf.WoomConfigError):
            wconf.load_cfg(str(cfg_file), str(spec_file))


class TestConfigManipulation:
    """Test configuration manipulation functions"""

    def test_strip_out_sections(self):
        cfg = configobj.ConfigObj()
        cfg["scalar1"] = "value1"
        cfg["scalar2"] = "value2"
        cfg["section"] = {"key": "value"}

        result = wconf.strip_out_sections(cfg)
        assert "scalar1" in result
        assert "scalar2" in result
        assert "section" not in result

    def test_keep_sections(self):
        cfg = configobj.ConfigObj()
        cfg["scalar1"] = "value1"
        cfg["section"] = {"key": "value"}

        result = wconf.keep_sections(cfg)
        assert "scalar1" not in result
        assert "section" in result

    def test_merge_args_with_config(self):
        cfg = configobj.ConfigObj()
        cfg["key1"] = "old_value"

        class Args:
            key1 = "new_value"
            key2 = "another_value"

        args = Args()
        wconf.merge_args_with_config(cfg, args, ["key1", "key2"])

        assert cfg["key1"] == "new_value"
        assert cfg["key2"] == "another_value"

    def test_merge_args_with_config_prefix(self):
        cfg = configobj.ConfigObj()
        cfg["name"] = "old_name"

        class Args:
            app_name = "new_name"

        args = Args()
        wconf.merge_args_with_config(cfg, args, ["name"], prefix="app_")

        assert cfg["name"] == "new_name"

    def test_inherit_cfg(self):
        cfg = configobj.ConfigObj()
        cfg["key1"] = "value1"

        inherit_from = configobj.ConfigObj()
        inherit_from["key1"] = "old_value"
        inherit_from["key2"] = "value2"

        wconf.inherit_cfg(cfg, inherit_from)

        assert cfg["key1"] == "value1"  # Not overridden
        assert cfg["key2"] == "value2"  # Inherited

    def test_inherit_cfg_recursive(self):
        cfg = configobj.ConfigObj()
        cfg["section"] = {"key1": "value1"}

        inherit_from = configobj.ConfigObj()
        inherit_from["section"] = {"key1": "old", "key2": "value2"}

        wconf.inherit_cfg(cfg, inherit_from)

        assert cfg["section"]["key1"] == "value1"
        assert cfg["section"]["key2"] == "value2"
