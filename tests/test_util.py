#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for woom.util module

Place this file at the root of your project (same level as woom/ directory)
Run: pytest test_util.py -v
"""

import json
import os
from unittest.mock import Mock

import pandas as pd
import pytest

from woom.util import (
    COLORS,
    WoomDate,
    WoomJSONEncoder,
    check_dir,
    colorize,
    dict_to_env_vars,
    flatten,
    pages2ints,
    set_deep_item,
)


class TestWoomDate:
    """Test WoomDate class"""

    def test_init_from_string(self):
        """Test WoomDate initialization from string"""
        date = WoomDate('2020-01-01')

        assert isinstance(date, pd.Timestamp)
        assert date.year == 2020
        assert date.month == 1
        assert date.day == 1

    def test_init_with_round(self):
        """Test WoomDate with rounding"""
        date = WoomDate('2020-01-01 12:34:56', round='h')

        assert date.hour == 13
        assert date.minute == 0
        assert date.second == 0

    def test_init_now(self):
        """Test WoomDate with 'now'"""
        date = WoomDate('now')

        assert isinstance(date, pd.Timestamp)

    def test_init_today(self):
        """Test WoomDate with 'today'"""
        date = WoomDate('today')

        assert isinstance(date, pd.Timestamp)

    def test_format_since(self):
        """Test format with 'since' specification"""
        date = WoomDate('2020-01-02')
        result = format(date, 'days since 2020-01-01')

        assert result == '1'

    def test_add_timedelta(self):
        """Test add method with timedelta"""
        date = WoomDate('2020-01-01')
        new_date = date.add('1D')

        assert new_date.day == 2

    def test_add_kwargs(self):
        """Test add method with kwargs"""
        date = WoomDate('2020-01-01')
        new_date = date.add(days=1, hours=2)

        assert new_date.day == 2
        assert new_date.hour == 2


class TestCheckDir:
    """Test check_dir function"""

    def test_check_dir_creates_directory(self, tmp_path):
        """Test that check_dir creates missing directory"""
        filepath = tmp_path / 'subdir' / 'file.txt'

        result = check_dir(str(filepath))

        assert os.path.exists(tmp_path / 'subdir')
        assert result == str(filepath)

    def test_check_dir_existing_directory(self, tmp_path):
        """Test check_dir with existing directory"""
        filepath = tmp_path / 'file.txt'

        result = check_dir(str(filepath))

        assert result == str(filepath)

    def test_check_dir_dry_mode(self, tmp_path):
        """Test check_dir in dry mode"""
        filepath = tmp_path / 'subdir' / 'file.txt'

        result = check_dir(str(filepath), dry=True)

        assert not os.path.exists(tmp_path / 'subdir')
        assert result == str(filepath)

    def test_check_dir_with_logger(self, tmp_path):
        """Test check_dir with logger"""
        filepath = tmp_path / 'subdir' / 'file.txt'
        mock_logger = Mock()

        check_dir(str(filepath), logger=mock_logger)

        mock_logger.debug.assert_called()
        mock_logger.info.assert_called()


class TestWoomJSONEncoder:
    """Test WoomJSONEncoder class"""

    def test_encode_userdict(self):
        """Test encoding UserDict"""
        from collections import UserDict

        data = UserDict({'key': 'value'})

        result = json.dumps(data, cls=WoomJSONEncoder)

        assert json.loads(result) == {'key': 'value'}

    def test_encode_timestamp(self):
        """Test encoding Timestamp"""
        date = pd.Timestamp('2020-01-01')

        result = json.dumps({'date': date}, cls=WoomJSONEncoder)

        assert '2020-01-01' in result

    def test_encode_timedelta(self):
        """Test encoding Timedelta"""
        delta = pd.Timedelta('1 days')

        result = json.dumps({'delta': delta}, cls=WoomJSONEncoder)

        assert 'P1DT' in result or '1 days' in result

    def test_encode_unknown_type(self):
        """Test encoding unknown type converts to string"""

        class CustomClass:
            def __str__(self):
                return "custom"

        obj = CustomClass()
        result = json.dumps({'obj': obj}, cls=WoomJSONEncoder)

        assert 'custom' in result


class TestDictToEnvVars:
    """Test dict_to_env_vars function"""

    def test_dict_to_env_vars_basic(self):
        """Test basic conversion"""
        items = {'key': 'value', 'number': 42}

        result = dict_to_env_vars(items)

        assert result['WOOM_KEY'] == 'value'
        assert result['WOOM_NUMBER'] == '42'

    def test_dict_to_env_vars_with_select(self):
        """Test conversion with select"""
        items = {'key1': 'value1', 'key2': 'value2'}

        result = dict_to_env_vars(items, select=['key1'])

        assert 'WOOM_KEY1' in result
        assert 'WOOM_KEY2' not in result

    def test_dict_to_env_vars_with_exclude(self):
        """Test conversion with exclude"""
        items = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}

        result = dict_to_env_vars(items, exclude=['key2'])

        assert 'WOOM_KEY1' in result
        assert 'WOOM_KEY2' not in result
        assert 'WOOM_KEY3' in result

    def test_dict_to_env_vars_timestamp(self):
        """Test conversion of Timestamp"""
        items = {'date': pd.Timestamp('2020-01-01')}

        result = dict_to_env_vars(items)

        assert 'WOOM_DATE' in result
        assert '2020-01-01' in result['WOOM_DATE']

    def test_dict_to_env_vars_bool(self):
        """Test conversion of boolean"""
        items = {'flag': True, 'other': False}

        result = dict_to_env_vars(items)

        assert result['WOOM_FLAG'] == '1'
        assert result['WOOM_OTHER'] == '0'

    def test_dict_to_env_vars_none(self):
        """Test conversion of None"""
        items = {'empty': None}

        result = dict_to_env_vars(items)

        assert result['WOOM_EMPTY'] == ''

    def test_dict_to_env_vars_nested(self):
        """Test conversion of nested dict"""
        items = {'top_key': 'top_value', 'params': {'key1': 'value1', 'key2': 42}}

        result = dict_to_env_vars(items)

        assert result['WOOM_TOP_KEY'] == 'top_value'
        assert result['WOOM_PARAMS_KEY1'] == 'value1'
        assert result['WOOM_PARAMS_KEY2'] == '42'

    def test_dict_to_env_vars_deeply_nested(self):
        """Test conversion of deeply nested dict"""
        items = {'level1': {'level2': {'level3': 'deep_value'}}}

        result = dict_to_env_vars(items)

        assert result['WOOM_LEVEL1_LEVEL2_LEVEL3'] == 'deep_value'

    def test_dict_to_env_vars_custom_prefix(self):
        """Test conversion with custom prefix"""
        items = {'key': 'value'}

        result = dict_to_env_vars(items, prefix='CUSTOM_')

        assert result['CUSTOM_KEY'] == 'value'
        assert 'WOOM_KEY' not in result

    def test_dict_to_env_vars_empty_prefix(self):
        """Test that empty prefix raises ValueError"""
        items = {'key': 'value'}

        with pytest.raises(ValueError, match="prefix cannot be empty"):
            dict_to_env_vars(items, prefix='')

    def test_dict_to_env_vars_invalid_prefix_type(self):
        """Test that non-string prefix raises TypeError"""
        items = {'key': 'value'}

        with pytest.raises(TypeError, match="prefix must be a string"):
            dict_to_env_vars(items, prefix=123)

    def test_dict_to_env_vars_nested_with_exclude(self):
        """Test nested conversion with exclude"""
        items = {
            'keep': 'value1',
            'exclude_me': 'value2',
            'nested': {'keep': 'value3', 'exclude_me': 'value4'},
        }

        result = dict_to_env_vars(items, exclude=['exclude_me'])

        assert 'WOOM_KEEP' in result
        assert 'WOOM_EXCLUDE_ME' not in result
        assert 'WOOM_NESTED_KEEP' in result
        # Note: exclude applies recursively to nested dicts
        assert 'WOOM_NESTED_EXCLUDE_ME' not in result

    def test_dict_to_env_vars_list(self):
        """Test conversion of list values"""
        items = {'paths': ['/usr/bin', '/usr/local/bin', '/opt/bin']}

        result = dict_to_env_vars(items)

        # Lists should be joined with os.pathsep (: on Unix, ; on Windows)
        assert 'WOOM_PATHS' in result
        assert '/usr/bin' in result['WOOM_PATHS']
        assert '/usr/local/bin' in result['WOOM_PATHS']
        assert os.pathsep in result['WOOM_PATHS']

    def test_dict_to_env_vars_nested_with_list(self):
        """Test nested dict containing list values"""
        items = {'config': {'search_paths': ['/path1', '/path2'], 'enabled': True}}

        result = dict_to_env_vars(items)

        assert 'WOOM_CONFIG_SEARCH_PATHS' in result
        assert '/path1' in result['WOOM_CONFIG_SEARCH_PATHS']
        assert 'WOOM_CONFIG_ENABLED' in result
        assert result['WOOM_CONFIG_ENABLED'] == '1'

    def test_dict_to_env_vars_empty_nested(self):
        """Test empty nested dict"""
        items = {'top': 'value', 'nested': {}}

        result = dict_to_env_vars(items)

        assert result['WOOM_TOP'] == 'value'
        # Empty dict should not create any env vars
        assert not any(k.startswith('WOOM_NESTED_') for k in result)

    def test_dict_to_env_vars_mixed_nesting(self):
        """Test complex nesting with different types"""
        items = {
            'simple': 'value',
            'database': {'host': 'localhost', 'port': 5432, 'replicas': ['db1', 'db2', 'db3']},
            'flags': {'debug': True, 'verbose': False},
        }

        result = dict_to_env_vars(items)

        assert result['WOOM_SIMPLE'] == 'value'
        assert result['WOOM_DATABASE_HOST'] == 'localhost'
        assert result['WOOM_DATABASE_PORT'] == '5432'
        assert 'db1' in result['WOOM_DATABASE_REPLICAS']
        assert result['WOOM_FLAGS_DEBUG'] == '1'
        assert result['WOOM_FLAGS_VERBOSE'] == '0'

    def test_dict_to_env_vars_configobj(self):
        """Test conversion of ConfigObj instances"""
        from configobj import ConfigObj

        config = ConfigObj()
        config['app'] = {}
        config['app']['name'] = 'test'
        config['app']['version'] = '1.0'
        config['debug'] = 'true'

        result = dict_to_env_vars(config)

        assert result['WOOM_APP_NAME'] == 'test'
        assert result['WOOM_APP_VERSION'] == '1.0'
        assert result['WOOM_DEBUG'] == 'true'

    def test_dict_to_env_vars_nested_configobj(self):
        """Test conversion with nested ConfigObj in dict"""
        from configobj import ConfigObj

        nested_config = ConfigObj()
        nested_config['host'] = 'localhost'
        nested_config['port'] = '5432'

        items = {'database': nested_config, 'app': 'myapp'}

        result = dict_to_env_vars(items)

        assert result['WOOM_DATABASE_HOST'] == 'localhost'
        assert result['WOOM_DATABASE_PORT'] == '5432'
        assert result['WOOM_APP'] == 'myapp'


class TestPages2Ints:
    """Test pages2ints function"""

    def test_pages2ints_single(self):
        """Test with single page"""
        pages = [3]

        result = pages2ints(pages, 10)

        assert result == [3]

    def test_pages2ints_with_slice(self):
        """Test with slice"""
        pages = [slice(2, 5)]

        result = pages2ints(pages, 10)

        assert result == [3, 4, 5]

    def test_pages2ints_mixed(self):
        """Test with mixed pages and slices"""
        pages = [1, slice(3, 5), 8]

        result = pages2ints(pages, 10)

        assert result == [1, 4, 5, 8]


class TestColorize:
    """Test colorize function"""

    def test_colorize_match(self):
        """Test colorize with matching pattern"""
        text = "ERROR"
        mapping = {"ERROR": "red"}

        result = colorize(text, mapping, colorize=True)

        # Should contain color codes when colorize=True and tty
        assert isinstance(result, str)

    def test_colorize_no_match(self):
        """Test colorize with no matching pattern"""
        text = "INFO"
        mapping = {"ERROR": "red"}

        result = colorize(text, mapping, colorize=True)

        assert result == "INFO"

    def test_colorize_disabled(self):
        """Test colorize when disabled"""
        text = "ERROR"
        mapping = {"ERROR": "red"}

        result = colorize(text, mapping, colorize=False)

        assert result == "ERROR"

    def test_colorize_combined_colors(self):
        """Test colorize with combined colors"""
        text = "CRITICAL"
        mapping = {"CRITICAL": "bold_red"}

        result = colorize(text, mapping, colorize=True)

        assert isinstance(result, str)


class TestFlatten:
    """Test flatten function"""

    def test_flatten_dict(self):
        """Test flatten with dict"""
        data = {'a': 1, 'b': 2}

        result = flatten(data)

        assert result == [1, 2]

    def test_flatten_list(self):
        """Test flatten with list"""
        data = [1, 2, 3]

        result = flatten(data)

        assert result == [1, 2, 3]

    def test_flatten_nested_dict(self):
        """Test flatten with nested dict"""
        data = {'a': {'b': 1, 'c': 2}, 'd': 3}

        result = flatten(data)

        assert set(result) == {1, 2, 3}

    def test_flatten_nested_list(self):
        """Test flatten with nested list"""
        data = [[1, 2], [3, 4]]

        result = flatten(data)

        assert result == [1, 2, 3, 4]

    def test_flatten_mixed(self):
        """Test flatten with mixed structures"""
        data = {'a': [1, 2], 'b': {'c': 3}}

        result = flatten(data)

        assert set(result) == {1, 2, 3}


class TestSetDeepItem:
    """Test set_deep_item function"""

    def test_set_deep_item_single_key(self):
        """Test with single key"""
        dd = {}

        set_deep_item(dd, 'value', 'key')

        assert dd == {'key': 'value'}

    def test_set_deep_item_nested_keys(self):
        """Test with nested keys"""
        dd = {}

        set_deep_item(dd, 'value', 'key1', 'key2', 'key3')

        assert dd == {'key1': {'key2': {'key3': 'value'}}}

    def test_set_deep_item_with_none(self):
        """Test with None key (should be ignored)"""
        dd = {}

        set_deep_item(dd, 'value', 'key1', None, 'key2')

        assert dd == {'key1': {'key2': 'value'}}

    def test_set_deep_item_existing_dict(self):
        """Test with existing dict"""
        dd = {'key1': {'existing': 'data'}}

        set_deep_item(dd, 'value', 'key1', 'key2')

        assert dd == {'key1': {'existing': 'data', 'key2': 'value'}}


class TestColors:
    """Test COLORS constant"""

    def test_colors_defined(self):
        """Test that COLORS is defined"""
        assert isinstance(COLORS, dict)
        assert 'red' in COLORS
        assert 'green' in COLORS
        assert 'yellow' in COLORS
        assert 'bold' in COLORS
        assert 'reset' in COLORS
