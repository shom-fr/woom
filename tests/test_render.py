#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for render.py module
"""
import os

import pytest

from woom import render as wrender
from woom import util as wutil


class TestRender:
    """Test template rendering"""

    def test_render_simple_string(self):
        template = "Hello {{ name }}"
        params = {"name": "World"}
        result = wrender.render(template, params)
        assert result == "Hello World"

    def test_render_multiple_vars(self):
        template = "{{ greeting }} {{ name }}"
        params = {"greeting": "Hello", "name": "Alice"}
        result = wrender.render(template, params)
        assert result == "Hello Alice"

    def test_render_nested(self):
        template = "{{ message }}"
        params = {"message": "Hello {{ name }}", "name": "Bob"}
        result = wrender.render(template, params, nested=True)
        assert result == "Hello Bob"

    def test_render_nested_disabled(self):
        template = "{{ message }}"
        params = {"message": "Hello {{ name }}", "name": "Bob"}
        result = wrender.render(template, params, nested=False)
        assert result == "Hello {{ name }}"

    def test_render_strict_mode(self):
        template = "{{ undefined_var }}"
        params = {}
        with pytest.raises(Exception):
            wrender.render(template, params, strict=True)

    def test_render_non_strict_mode(self):
        template = "{{ undefined_var }}"
        params = {}
        result = wrender.render(template, params, strict=False)
        assert result is not None

    def test_render_from_template_object(self):
        tpl = wrender.JINJA_ENV.from_string("Value: {{ value }}")
        params = {"value": 42}
        result = wrender.render(tpl, params)
        assert result == "Value: 42"


class TestFilterReplicateOption:
    """Test replicate_option filter"""

    def test_replicate_single_value(self):
        result = wrender.filter_replicate_option("value", "--opt")
        assert result == "--opt=value"

    def test_replicate_multiple_values(self):
        result = wrender.filter_replicate_option(["val1", "val2"], "--opt")
        assert "--opt=val1" in result
        assert "--opt=val2" in result

    def test_replicate_custom_format(self):
        result = wrender.filter_replicate_option(["a", "b"], "--option", format="{opt_name} {value}")
        assert "--option a" in result
        assert "--option b" in result

    def test_replicate_with_spaces(self):
        result = wrender.filter_replicate_option(["value with space"], "--opt")
        assert result is not None


class TestFilterStrftime:
    """Test strftime filter"""

    def test_strftime_year_month(self):
        result = wrender.filter_strftime("2025-01-15", "%Y-%m")
        assert result == "2025-01"

    def test_strftime_full_date(self):
        result = wrender.filter_strftime("2025-01-15", "%Y-%m-%d")
        assert result == "2025-01-15"

    def test_strftime_with_timestamp(self):
        date = wutil.WoomDate("2025-01-15 14:30:00")
        result = wrender.filter_strftime(date, "%Y-%m-%d %H:%M")
        assert result == "2025-01-15 14:30"

    def test_strftime_day_name(self):
        result = wrender.filter_strftime("2025-01-15", "%A")
        assert result == "Wednesday"


class TestFilterAsEnvStr:
    """Test as_env_str filter"""

    def test_as_env_str_scalar(self):
        result = wrender.filter_as_env_str(42)
        assert result == "42"

    def test_as_env_str_string(self):
        result = wrender.filter_as_env_str("test")
        assert result == "test"

    def test_as_env_str_list(self):
        result = wrender.filter_as_env_str(["path1", "path2"])
        expected = os.pathsep.join(["path1", "path2"])
        assert result == expected

    def test_as_env_str_tuple(self):
        result = wrender.filter_as_env_str(("val1", "val2"))
        assert os.pathsep in result

    def test_as_env_str_set(self):
        result = wrender.filter_as_env_str({"val1", "val2"})
        assert os.pathsep in result or len(result.split(os.pathsep)) == 2


class TestJinjaFilters:
    """Test that filters are registered in Jinja environment"""

    def test_filters_registered(self):
        assert "replicate_option" in wrender.JINJA_ENV.filters
        assert "strftime" in wrender.JINJA_ENV.filters
        assert "as_str_env" in wrender.JINJA_ENV.filters

    def test_filter_in_template_replicate(self):
        template = "{{ values|replicate_option('--var') }}"
        tpl = wrender.JINJA_ENV.from_string(template)
        result = tpl.render(values=["a", "b"])
        assert "--var=a" in result
        assert "--var=b" in result

    def test_filter_in_template_strftime(self):
        template = "{{ date|strftime('%Y-%m') }}"
        tpl = wrender.JINJA_ENV.from_string(template)
        result = tpl.render(date="2025-01-15")
        assert result == "2025-01"

    def test_filter_in_template_as_str_env(self):
        template = "{{ paths|as_str_env }}"
        tpl = wrender.JINJA_ENV.from_string(template)
        result = tpl.render(paths=["path1", "path2"])
        assert os.pathsep in result


class TestJinjaEnv:
    """Test Jinja environment configuration"""

    def test_jinja_env_exists(self):
        assert wrender.JINJA_ENV is not None

    def test_jinja_env_trim_blocks(self):
        assert wrender.JINJA_ENV.trim_blocks is True

    def test_jinja_env_strict_undefined(self):
        template = wrender.JINJA_ENV.from_string("{{ undefined }}")
        with pytest.raises(Exception):
            template.render()
