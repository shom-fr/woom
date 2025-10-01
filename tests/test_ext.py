#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for ext.py module
"""
import sys

from woom import conf as wconf
from woom import ext as wext
from woom import render as wrender


class TestImportFromPath:
    """Test import_from_path function"""

    def test_import_from_path(self, tmp_path):
        # Create a simple Python module
        module_file = tmp_path / "test_module.py"
        module_file.write_text(
            """
def test_function():
    return "Hello from module"

TEST_VALUE = 42
"""
        )

        module = wext.import_from_path("test_import", str(module_file))

        assert module is not None
        assert hasattr(module, "test_function")
        assert module.test_function() == "Hello from module"
        assert module.TEST_VALUE == 42

    def test_import_from_path_in_sys_modules(self, tmp_path):
        module_file = tmp_path / "test_sys.py"
        module_file.write_text("VALUE = 123")

        module_name = "test_sys_module"
        module = wext.import_from_path(module_name, str(module_file))

        assert module_name in sys.modules
        assert sys.modules[module_name] is module


class TestLoadExtensions:
    """Test load_extensions function"""

    def test_load_extensions_no_ext_dir(self, tmp_path):
        workflow_dir = tmp_path / "workflow"
        workflow_dir.mkdir()

        exts = wext.load_extensions(str(workflow_dir))
        assert exts == []

    def test_load_extensions_empty_ext_dir(self, tmp_path):
        workflow_dir = tmp_path / "workflow"
        workflow_dir.mkdir()
        ext_dir = workflow_dir / "ext"
        ext_dir.mkdir()

        exts = wext.load_extensions(str(workflow_dir))
        assert exts == []

    def test_load_extensions_jinja_filters(self, tmp_path):
        workflow_dir = tmp_path / "workflow"
        workflow_dir.mkdir()
        ext_dir = workflow_dir / "ext"
        ext_dir.mkdir()

        # Create jinja filters extension
        jinja_ext = ext_dir / "jinja_filters.py"
        jinja_ext.write_text(
            """
def custom_filter(value):
    return f"custom_{value}"

JINJA_FILTERS = {
    "custom": custom_filter
}
"""
        )

        exts = wext.load_extensions(str(workflow_dir))

        assert "jinja_filters" in exts
        assert "custom" in wrender.JINJA_ENV.filters

    def test_load_extensions_validator_functions(self, tmp_path):
        workflow_dir = tmp_path / "workflow"
        workflow_dir.mkdir()
        ext_dir = workflow_dir / "ext"
        ext_dir.mkdir()

        # Create validator functions extension
        vf_ext = ext_dir / "validator_functions.py"
        vf_ext.write_text(
            """
def is_positive(value):
    if int(value) > 0:
        return int(value)
    raise ValueError("Must be positive")

VALIDATOR_FUNCTIONS = {
    "positive": is_positive
}
"""
        )

        exts = wext.load_extensions(str(workflow_dir))

        assert "validator_functions" in exts
        assert "positive" in wconf.VALIDATOR_FUNCTIONS

    def test_load_extensions_both(self, tmp_path):
        workflow_dir = tmp_path / "workflow"
        workflow_dir.mkdir()
        ext_dir = workflow_dir / "ext"
        ext_dir.mkdir()

        # Jinja filters
        jinja_ext = ext_dir / "jinja_filters.py"
        jinja_ext.write_text(
            """
JINJA_FILTERS = {"test": lambda x: x}
"""
        )

        # Validator functions
        vf_ext = ext_dir / "validator_functions.py"
        vf_ext.write_text(
            """
VALIDATOR_FUNCTIONS = {"test": lambda x: x}
"""
        )

        exts = wext.load_extensions(str(workflow_dir))

        assert len(exts) == 2
        assert "jinja_filters" in exts
        assert "validator_functions" in exts


class TestLoadJinjaFilters:
    """Test load_jinja_filters function"""

    def test_load_jinja_filters_valid(self, tmp_path):
        ext_file = tmp_path / "filters.py"
        ext_file.write_text(
            """
def uppercase_filter(value):
    return str(value).upper()

JINJA_FILTERS = {
    "uppercase": uppercase_filter
}
"""
        )

        result = wext.load_jinja_filters(str(ext_file))

        assert result == "jinja_filters"
        assert "uppercase" in wrender.JINJA_ENV.filters

    def test_load_jinja_filters_no_attribute(self, tmp_path):
        ext_file = tmp_path / "filters.py"
        ext_file.write_text(
            """
def some_function():
    pass
"""
        )

        result = wext.load_jinja_filters(str(ext_file))
        assert result is None

    def test_load_jinja_filters_empty_dict(self, tmp_path):
        ext_file = tmp_path / "filters.py"
        ext_file.write_text(
            """
JINJA_FILTERS = {}
"""
        )

        result = wext.load_jinja_filters(str(ext_file))
        assert result == "jinja_filters"


class TestLoadValidatorFunctions:
    """Test load_validator_functions function"""

    def test_load_validator_functions_valid(self, tmp_path):
        ext_file = tmp_path / "validators.py"
        ext_file.write_text(
            """
def is_even(value):
    val = int(value)
    if val % 2 == 0:
        return val
    raise ValueError("Must be even")

VALIDATOR_FUNCTIONS = {
    "even": is_even
}
"""
        )

        result = wext.load_validator_functions(str(ext_file))

        assert result == "validator_functions"
        assert "even" in wconf.VALIDATOR_FUNCTIONS

    def test_load_validator_functions_no_attribute(self, tmp_path):
        ext_file = tmp_path / "validators.py"
        ext_file.write_text(
            """
def some_function():
    pass
"""
        )

        result = wext.load_validator_functions(str(ext_file))
        assert result is None

    def test_load_validator_functions_empty_dict(self, tmp_path):
        ext_file = tmp_path / "validators.py"
        ext_file.write_text(
            """
VALIDATOR_FUNCTIONS = {}
"""
        )

        result = wext.load_validator_functions(str(ext_file))
        assert result == "validator_functions"


class TestExtensionsIntegration:
    """Integration tests for extensions"""

    def test_jinja_filter_usage(self, tmp_path):
        workflow_dir = tmp_path / "workflow"
        workflow_dir.mkdir()
        ext_dir = workflow_dir / "ext"
        ext_dir.mkdir()

        jinja_ext = ext_dir / "jinja_filters.py"
        jinja_ext.write_text(
            """
def reverse_filter(value):
    return str(value)[::-1]

JINJA_FILTERS = {
    "reverse": reverse_filter
}
"""
        )

        wext.load_extensions(str(workflow_dir))

        # Use the filter
        template = wrender.JINJA_ENV.from_string("{{ text|reverse }}")
        result = template.render(text="hello")
        assert result == "olleh"

    def test_validator_function_usage(self, tmp_path):
        workflow_dir = tmp_path / "workflow"
        workflow_dir.mkdir()
        ext_dir = workflow_dir / "ext"
        ext_dir.mkdir()

        vf_ext = ext_dir / "validator_functions.py"
        vf_ext.write_text(
            """
def is_uppercase(value):
    if value == value.upper():
        return value
    raise ValueError("Must be uppercase")

VALIDATOR_FUNCTIONS = {
    "uppercase": is_uppercase
}
"""
        )

        wext.load_extensions(str(workflow_dir))

        # Use the validator
        validator = wconf.get_validator()
        assert "uppercase" in validator.functions
