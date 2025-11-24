#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for woom.env module

Place this file at the root of your project (same level as woom/ directory)
Run: pytest test_env.py -v
"""
import os

import pytest

from woom.env import EnvConfig


class TestEnvConfigInit:
    """Test EnvConfig initialization"""

    def test_init_empty(self):
        """Test initialization with no arguments"""
        env = EnvConfig()

        assert env.raw_text is None
        assert env.vars_forward == []
        assert env.vars_set == {}
        assert env.vars_append == {}
        assert env.vars_prepend == {}
        assert env.module_setup is None
        assert env.module_use is None
        assert env.module_load is None
        assert env.conda_setup is None
        assert env.conda_activate is None

    def test_init_with_raw_text(self):
        """Test initialization with raw_text"""
        env = EnvConfig(raw_text="export VAR=value")

        assert env.raw_text == "export VAR=value"

    def test_init_with_vars_forward(self):
        """Test initialization with vars_forward"""
        env = EnvConfig(vars_forward=['PATH', 'HOME'])

        assert env.vars_forward == ['PATH', 'HOME']

    def test_init_with_vars_set(self):
        """Test initialization with vars_set"""
        env = EnvConfig(vars_set={'VAR1': 'value1', 'VAR2': 'value2'})

        assert env.vars_set == {'VAR1': 'value1', 'VAR2': 'value2'}

    def test_init_with_paths(self):
        """Test initialization with path variables"""
        env = EnvConfig(vars_append={'PATH': '/usr/local/bin'}, vars_prepend={'LD_LIBRARY_PATH': '/opt/lib'})

        assert '/usr/local/bin' in str(env.vars_append.get('PATH', []))
        assert '/opt/lib' in str(env.vars_prepend.get('LD_LIBRARY_PATH', []))

    def test_init_with_modules(self):
        """Test initialization with module configuration"""
        env = EnvConfig(
            module_setup='source /etc/profile.d/modules.sh',
            module_use='/opt/modules',
            module_load='gcc/11.2.0',
        )

        assert env.module_setup == 'source /etc/profile.d/modules.sh'
        assert env.module_use == '/opt/modules'
        assert env.module_load == 'gcc/11.2.0'

    def test_init_with_conda(self):
        """Test initialization with conda configuration"""
        env = EnvConfig(conda_setup='source ~/miniconda3/etc/profile.d/conda.sh', conda_activate='myenv')

        assert env.conda_setup == 'source ~/miniconda3/etc/profile.d/conda.sh'
        assert env.conda_activate == 'myenv'


class TestEnvConfigPathMethods:
    """Test path manipulation methods"""

    def test_append_paths_single(self):
        """Test append_paths with single path"""
        env = EnvConfig()

        env.append_paths(PATH='/usr/local/bin')

        assert 'PATH' in env.vars_append
        assert '/usr/local/bin' in env.vars_append['PATH']

    def test_append_paths_multiple(self):
        """Test append_paths with multiple paths"""
        env = EnvConfig()

        env.append_paths(PATH='/usr/local/bin', LD_LIBRARY_PATH='/opt/lib')

        assert 'PATH' in env.vars_append
        assert 'LD_LIBRARY_PATH' in env.vars_append

    def test_append_paths_with_pathsep(self):
        """Test append_paths with os.pathsep separated string"""
        env = EnvConfig()
        paths = f'/usr/local/bin{os.pathsep}/usr/bin'

        env.append_paths(PATH=paths)

        assert '/usr/local/bin' in env.vars_append['PATH']
        assert '/usr/bin' in env.vars_append['PATH']

    def test_append_paths_cumulative(self):
        """Test that append_paths is cumulative"""
        env = EnvConfig()

        env.append_paths(PATH='/usr/local/bin')
        env.append_paths(PATH='/opt/bin')

        assert '/usr/local/bin' in env.vars_append['PATH']
        assert '/opt/bin' in env.vars_append['PATH']

    def test_prepend_paths_single(self):
        """Test prepend_paths with single path"""
        env = EnvConfig()

        env.prepend_paths(PATH='/usr/local/bin')

        assert 'PATH' in env.vars_prepend
        assert '/usr/local/bin' in env.vars_prepend['PATH']

    def test_prepend_paths_multiple(self):
        """Test prepend_paths with multiple paths"""
        env = EnvConfig()

        env.prepend_paths(PATH='/usr/local/bin', PYTHONPATH='/opt/python')

        assert 'PATH' in env.vars_prepend
        assert 'PYTHONPATH' in env.vars_prepend

    def test_prepend_paths_cumulative(self):
        """Test that prepend_paths is cumulative"""
        env = EnvConfig()

        env.prepend_paths(PATH='/usr/local/bin')
        env.prepend_paths(PATH='/opt/bin')

        assert '/usr/local/bin' in env.vars_prepend['PATH']
        assert '/opt/bin' in env.vars_prepend['PATH']

    def test_set_paths(self):
        """Test set_paths method"""
        env = EnvConfig()

        env.set_paths(PATH='/usr/bin', HOME='/home/user')

        assert 'PATH' in env.vars_set
        assert 'HOME' in env.vars_set


class TestEnvConfigHelpers:
    """Test helper methods"""

    def test_has_vars_empty(self):
        """Test has_vars with no variables"""
        env = EnvConfig()

        assert not env.has_vars()

    def test_has_vars_forward(self):
        """Test has_vars with vars_forward"""
        env = EnvConfig(vars_forward=['PATH'])

        assert env.has_vars()

    def test_has_vars_set(self):
        """Test has_vars with vars_set"""
        env = EnvConfig(vars_set={'VAR': 'value'})

        assert env.has_vars()

    def test_has_vars_append(self):
        """Test has_vars with vars_append"""
        env = EnvConfig()
        env.append_paths(PATH='/usr/bin')

        assert env.has_vars()

    def test_has_vars_prepend(self):
        """Test has_vars with vars_prepend"""
        env = EnvConfig()
        env.prepend_paths(PATH='/usr/bin')

        assert env.has_vars()

    def test_as_string_scalar(self):
        """Test _as_string_ with scalar value"""
        result = EnvConfig._as_string_('test')

        assert result == 'test'

    def test_as_string_list(self):
        """Test _as_string_ with list"""
        result = EnvConfig._as_string_(['/usr/bin', '/usr/local/bin'])

        assert '/usr/bin' in result
        assert '/usr/local/bin' in result
        assert os.pathsep in result

    def test_as_string_tuple(self):
        """Test _as_string_ with tuple"""
        result = EnvConfig._as_string_(('/usr/bin', '/usr/local/bin'))

        assert '/usr/bin' in result
        assert '/usr/local/bin' in result

    def test_check_path_string(self):
        """Test _check_path_ with string"""
        result = EnvConfig._check_path_(f'/usr/bin{os.pathsep}/usr/local/bin')

        assert isinstance(result, list)
        assert '/usr/bin' in result
        assert '/usr/local/bin' in result

    def test_check_path_list(self):
        """Test _check_path_ with list"""
        result = EnvConfig._check_path_(['/usr/bin', '/usr/local/bin'])

        assert isinstance(result, list)
        assert result == ['/usr/bin', '/usr/local/bin']


class TestEnvConfigCopy:
    """Test copy method"""

    def test_copy_basic(self):
        """Test copy creates a new instance"""
        env = EnvConfig(raw_text='test')
        env_copy = env.copy()

        assert isinstance(env_copy, EnvConfig)
        assert env_copy is not env
        assert env_copy.raw_text == env.raw_text

    def test_copy_with_vars(self):
        """Test copy preserves all variables"""
        env = EnvConfig(
            vars_forward=['PATH'],
            vars_set={'VAR': 'value'},
        )
        env.append_paths(PATH='/usr/bin')
        env.prepend_paths(LD_LIBRARY_PATH='/opt/lib')

        env_copy = env.copy()

        assert env_copy.vars_forward == env.vars_forward
        assert env_copy.vars_set == env.vars_set
        assert env_copy.vars_append == env.vars_append
        assert env_copy.vars_prepend == env.vars_prepend

    def test_copy_independence(self):
        """Test that copy is independent from original"""
        env = EnvConfig(vars_set={'VAR': 'value'})
        env_copy = env.copy()

        env_copy.vars_set['NEW_VAR'] = 'new_value'

        assert 'NEW_VAR' not in env.vars_set
        assert 'NEW_VAR' in env_copy.vars_set

    def test_copy_with_modules(self):
        """Test copy preserves module configuration"""
        env = EnvConfig(
            module_setup='setup',
            module_use='/modules',
            module_load='gcc',
            conda_setup='conda_setup',
            conda_activate='myenv',
            uv_venv=True,
        )

        env_copy = env.copy()

        assert env_copy.module_setup == env.module_setup
        assert env_copy.module_use == env.module_use
        assert env_copy.module_load == env.module_load
        assert env_copy.conda_setup == env.conda_setup
        assert env_copy.conda_activate == env.conda_activate
        assert env_copy.uv_venv == env.uv_venv


class TestEnvConfigIntegration:
    """Test integration scenarios"""

    def test_complex_environment(self):
        """Test creating a complex environment"""
        env = EnvConfig(
            raw_text='# Custom setup',
            vars_forward=['USER', 'HOME'],
            vars_set={'LANG': 'en_US.UTF-8'},
            module_setup='source /etc/profile.d/modules.sh',
            module_load='gcc/11.2.0 python/3.9',
        )

        env.append_paths(PATH='/usr/local/bin')
        env.prepend_paths(LD_LIBRARY_PATH='/opt/lib')

        assert env.has_vars()
        assert len(env.vars_forward) == 2
        assert 'LANG' in env.vars_set
        assert 'PATH' in env.vars_append
        assert 'LD_LIBRARY_PATH' in env.vars_prepend

    def test_incremental_path_building(self):
        """Test building paths incrementally"""
        env = EnvConfig()

        # Add paths in multiple steps
        env.prepend_paths(PATH='/opt/bin')
        env.prepend_paths(PATH='/usr/local/bin')
        env.append_paths(PATH='/usr/bin')
        env.append_paths(PATH='/bin')

        # Check all paths are present
        path_list = env.vars_prepend['PATH'] + env.vars_append['PATH']
        assert '/opt/bin' in path_list
        assert '/usr/local/bin' in path_list
        assert '/usr/bin' in path_list
        assert '/bin' in path_list
