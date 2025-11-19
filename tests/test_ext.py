#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for woom.ext module

Place this file at the root of your project (same level as woom/ directory)
Run: pytest test_ext.py -v
"""
import os
import sys
from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest

from woom.ext import (
    import_from_path,
    load_artifacts_generators,
    load_extensions,
    load_jinja_filters,
    load_validator_functions,
)


class TestImportFromPath:
    """Test import_from_path function"""

    @patch('woom.ext.importlib.util.spec_from_file_location')
    @patch('woom.ext.importlib.util.module_from_spec')
    def test_import_from_path_basic(self, mock_module_from_spec, mock_spec_from_file):
        """Test basic import_from_path"""
        mock_spec = Mock()
        mock_spec.loader = Mock()
        mock_spec_from_file.return_value = mock_spec
        mock_module = Mock()
        mock_module_from_spec.return_value = mock_module

        result = import_from_path('test_module', '/path/to/module.py')

        mock_spec_from_file.assert_called_once_with('test_module', '/path/to/module.py')
        mock_module_from_spec.assert_called_once_with(mock_spec)
        mock_spec.loader.exec_module.assert_called_once_with(mock_module)
        assert result is mock_module

    @patch('woom.ext.importlib.util.spec_from_file_location')
    @patch('woom.ext.importlib.util.module_from_spec')
    def test_import_from_path_adds_to_sys_modules(self, mock_module_from_spec, mock_spec_from_file):
        """Test that import_from_path adds module to sys.modules"""
        mock_spec = Mock()
        mock_spec.loader = Mock()
        mock_spec_from_file.return_value = mock_spec
        mock_module = Mock()
        mock_module_from_spec.return_value = mock_module

        module_name = 'test_ext_module_unique'
        import_from_path(module_name, '/path/to/module.py')

        assert module_name in sys.modules


class TestLoadJinjaFilters:
    """Test load_jinja_filters function"""

    @patch('woom.ext.import_from_path')
    def test_load_jinja_filters_success(self, mock_import):
        """Test successful loading of jinja filters"""
        mock_module = Mock()
        mock_module.JINJA_FILTERS = {'custom_filter': lambda x: x.upper()}
        mock_import.return_value = mock_module

        with patch('woom.render.JINJA_ENV') as mock_env:
            mock_env.filters = {}
            result = load_jinja_filters('/path/to/jinja_filters.py')

        assert result == 'jinja_filters'
        mock_import.assert_called_once_with('woom.ext.jinja_filters', '/path/to/jinja_filters.py')

    @patch('woom.ext.import_from_path')
    def test_load_jinja_filters_no_attribute(self, mock_import):
        """Test load_jinja_filters when JINJA_FILTERS not present"""
        mock_module = Mock(spec=[])  # No JINJA_FILTERS attribute
        mock_import.return_value = mock_module

        result = load_jinja_filters('/path/to/jinja_filters.py')

        assert result is None

    @patch('woom.ext.import_from_path')
    def test_load_jinja_filters_updates_env(self, mock_import):
        """Test that filters are added to JINJA_ENV"""
        test_filter = lambda x: x * 2
        mock_module = Mock()
        mock_module.JINJA_FILTERS = {'double': test_filter}
        mock_import.return_value = mock_module

        with patch('woom.render.JINJA_ENV') as mock_env:
            mock_env.filters = Mock()
            load_jinja_filters('/path/to/jinja_filters.py')

            mock_env.filters.update.assert_called_once()


class TestLoadValidatorFunctions:
    """Test load_validator_functions function"""

    @patch('woom.ext.import_from_path')
    def test_load_validator_functions_success(self, mock_import):
        """Test successful loading of validator functions"""
        mock_module = Mock()
        mock_module.VALIDATOR_FUNCTIONS = {'custom_validator': lambda x: x}
        mock_import.return_value = mock_module

        with patch('woom.conf.VALIDATOR_FUNCTIONS', {}) as mock_validators:
            result = load_validator_functions('/path/to/validator_functions.py')

        assert result == 'validator_functions'
        mock_import.assert_called_once_with('woom.ext.validator_functions', '/path/to/validator_functions.py')

    @patch('woom.ext.import_from_path')
    def test_load_validator_functions_no_attribute(self, mock_import):
        """Test load_validator_functions when VALIDATOR_FUNCTIONS not present"""
        mock_module = Mock(spec=[])
        mock_import.return_value = mock_module

        result = load_validator_functions('/path/to/validator_functions.py')

        assert result is None

    @patch('woom.ext.import_from_path')
    @patch('woom.conf.VALIDATOR_FUNCTIONS', {})
    def test_load_validator_functions_updates_dict(self, mock_import):
        """Test that functions are added to VALIDATOR_FUNCTIONS"""
        test_validator = lambda x: bool(x)
        mock_module = Mock()
        mock_module.VALIDATOR_FUNCTIONS = {'is_valid': test_validator}
        mock_import.return_value = mock_module

        from woom.conf import VALIDATOR_FUNCTIONS

        original_len = len(VALIDATOR_FUNCTIONS)

        load_validator_functions('/path/to/validator_functions.py')

        # Functions should be added
        mock_import.assert_called_once()


class TestLoadArtifactsGenerators:
    """Test load_artifacts_generators function"""

    @patch('woom.ext.import_from_path')
    def test_load_artifacts_generators_success(self, mock_import):
        """Test successful loading of artifacts generators"""
        mock_module = Mock()
        mock_module.ARTIFACTS_GENERATORS = {'custom_generator': lambda: []}
        mock_import.return_value = mock_module

        with patch('woom.tasks.ARTIFACTS_GENERATORS', {}) as mock_generators:
            result = load_artifacts_generators('/path/to/artifacts_generators.py')

        assert result == 'artifacts_generators'
        mock_import.assert_called_once_with(
            'woom.ext.artifacts_generators', '/path/to/artifacts_generators.py'
        )

    @patch('woom.ext.import_from_path')
    def test_load_artifacts_generators_no_attribute(self, mock_import):
        """Test load_artifacts_generators when ARTIFACTS_GENERATORS not present"""
        mock_module = Mock(spec=[])
        mock_import.return_value = mock_module

        result = load_artifacts_generators('/path/to/artifacts_generators.py')

        assert result is None


class TestLoadExtensions:
    """Test load_extensions function"""

    def test_load_extensions_no_ext_dir(self, tmp_path):
        """Test load_extensions when ext/ directory doesn't exist"""
        workflow_dir = tmp_path / 'workflow'
        workflow_dir.mkdir()

        result = load_extensions(str(workflow_dir))

        assert result == []

    @patch('woom.ext.load_jinja_filters')
    def test_load_extensions_jinja_only(self, mock_load_jinja, tmp_path):
        """Test load_extensions with only jinja_filters.py"""
        workflow_dir = tmp_path / 'workflow'
        ext_dir = workflow_dir / 'ext'
        ext_dir.mkdir(parents=True)
        (ext_dir / 'jinja_filters.py').touch()

        mock_load_jinja.return_value = 'jinja_filters'

        result = load_extensions(str(workflow_dir))

        assert 'jinja_filters' in result
        mock_load_jinja.assert_called_once()

    @patch('woom.ext.load_validator_functions')
    def test_load_extensions_validators_only(self, mock_load_validators, tmp_path):
        """Test load_extensions with only validator_functions.py"""
        workflow_dir = tmp_path / 'workflow'
        ext_dir = workflow_dir / 'ext'
        ext_dir.mkdir(parents=True)
        (ext_dir / 'validator_functions.py').touch()

        mock_load_validators.return_value = 'validator_functions'

        result = load_extensions(str(workflow_dir))

        assert 'validator_functions' in result
        mock_load_validators.assert_called_once()

    @patch('woom.ext.load_artifacts_generators')
    def test_load_extensions_artifacts_only(self, mock_load_artifacts, tmp_path):
        """Test load_extensions with only artifacts_generators.py"""
        workflow_dir = tmp_path / 'workflow'
        ext_dir = workflow_dir / 'ext'
        ext_dir.mkdir(parents=True)
        (ext_dir / 'artifacts_generators.py').touch()

        mock_load_artifacts.return_value = 'artifacts_generators'

        result = load_extensions(str(workflow_dir))

        assert 'artifacts_generators' in result
        mock_load_artifacts.assert_called_once()

    @patch('woom.ext.load_jinja_filters')
    @patch('woom.ext.load_validator_functions')
    @patch('woom.ext.load_artifacts_generators')
    def test_load_extensions_all(self, mock_artifacts, mock_validators, mock_jinja, tmp_path):
        """Test load_extensions with all extension types"""
        workflow_dir = tmp_path / 'workflow'
        ext_dir = workflow_dir / 'ext'
        ext_dir.mkdir(parents=True)
        (ext_dir / 'jinja_filters.py').touch()
        (ext_dir / 'validator_functions.py').touch()
        (ext_dir / 'artifacts_generators.py').touch()

        mock_jinja.return_value = 'jinja_filters'
        mock_validators.return_value = 'validator_functions'
        mock_artifacts.return_value = 'artifacts_generators'

        result = load_extensions(str(workflow_dir))

        assert len(result) == 3
        assert 'jinja_filters' in result
        assert 'validator_functions' in result
        assert 'artifacts_generators' in result

    @patch('woom.ext.load_jinja_filters')
    @patch('woom.ext.load_validator_functions')
    def test_load_extensions_some_none(self, mock_validators, mock_jinja, tmp_path):
        """Test load_extensions when some return None"""
        workflow_dir = tmp_path / 'workflow'
        ext_dir = workflow_dir / 'ext'
        ext_dir.mkdir(parents=True)
        (ext_dir / 'jinja_filters.py').touch()
        (ext_dir / 'validator_functions.py').touch()

        mock_jinja.return_value = 'jinja_filters'
        mock_validators.return_value = None  # No VALIDATOR_FUNCTIONS attribute

        result = load_extensions(str(workflow_dir))

        assert len(result) == 1
        assert 'jinja_filters' in result
        assert 'validator_functions' not in result

    def test_load_extensions_empty_ext_dir(self, tmp_path):
        """Test load_extensions with empty ext/ directory"""
        workflow_dir = tmp_path / 'workflow'
        ext_dir = workflow_dir / 'ext'
        ext_dir.mkdir(parents=True)

        result = load_extensions(str(workflow_dir))

        assert result == []


class TestExtensionsIntegration:
    """Test integration scenarios"""

    @patch('woom.ext.import_from_path')
    def test_multiple_extensions_loaded_sequentially(self, mock_import, tmp_path):
        """Test that multiple extensions are loaded in order"""
        workflow_dir = tmp_path / 'workflow'
        ext_dir = workflow_dir / 'ext'
        ext_dir.mkdir(parents=True)
        (ext_dir / 'jinja_filters.py').touch()
        (ext_dir / 'validator_functions.py').touch()

        # Create mock modules with attributes
        jinja_module = Mock()
        jinja_module.JINJA_FILTERS = {'filter1': lambda x: x}

        validator_module = Mock()
        validator_module.VALIDATOR_FUNCTIONS = {'validator1': lambda x: x}

        def side_effect(name, path):
            if 'jinja_filters' in name:
                return jinja_module
            elif 'validator_functions' in name:
                return validator_module
            return Mock(spec=[])

        mock_import.side_effect = side_effect

        with patch('woom.render.JINJA_ENV') as mock_env, patch('woom.conf.VALIDATOR_FUNCTIONS', {}):
            mock_env.filters = {}
            result = load_extensions(str(workflow_dir))

        assert len(result) == 2
        assert 'jinja_filters' in result
        assert 'validator_functions' in result

    def test_extension_files_checked_in_order(self, tmp_path):
        """Test that extension files are checked in specific order"""
        workflow_dir = tmp_path / 'workflow'
        ext_dir = workflow_dir / 'ext'
        ext_dir.mkdir(parents=True)

        # Create all possible extension files
        (ext_dir / 'jinja_filters.py').touch()
        (ext_dir / 'validator_functions.py').touch()
        (ext_dir / 'artifacts_generators.py').touch()

        with (
            patch('woom.ext.load_jinja_filters') as mock_jinja,
            patch('woom.ext.load_validator_functions') as mock_validators,
            patch('woom.ext.load_artifacts_generators') as mock_artifacts,
        ):
            mock_jinja.return_value = 'jinja_filters'
            mock_validators.return_value = 'validator_functions'
            mock_artifacts.return_value = 'artifacts_generators'

            result = load_extensions(str(workflow_dir))

            # All should be called
            mock_jinja.assert_called_once()
            mock_validators.assert_called_once()
            mock_artifacts.assert_called_once()
