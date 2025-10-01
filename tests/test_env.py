#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for env.py module
"""
import os

from woom import env as wenv


class TestEnvConfig:
    """Test EnvConfig class"""

    def test_init_empty(self):
        env = wenv.EnvConfig()
        assert env.vars_forward == []
        assert env.vars_set == {}
        assert env.vars_append == {}
        assert env.vars_prepend == {}

    def test_init_with_vars(self):
        env = wenv.EnvConfig(
            vars_forward=["PATH", "HOME"],
            vars_set={"MY_VAR": "value"},
        )
        assert "PATH" in env.vars_forward
        assert "HOME" in env.vars_forward
        assert env.vars_set["MY_VAR"] == "value"

    def test_init_with_paths(self):
        env = wenv.EnvConfig(
            vars_append={"PATH": "/usr/local/bin"}, vars_prepend={"LD_LIBRARY_PATH": "/opt/lib"}
        )
        assert "/usr/local/bin" in env.vars_append["PATH"]
        assert "/opt/lib" in env.vars_prepend["LD_LIBRARY_PATH"]

    def test_has_vars_true(self):
        env = wenv.EnvConfig(vars_set={"VAR": "value"})
        assert env.has_vars()

    def test_has_vars_false(self):
        env = wenv.EnvConfig()
        assert not env.has_vars()

    def test_append_paths_string(self):
        env = wenv.EnvConfig()
        env.append_paths(PATH="/new/path")
        assert "/new/path" in env.vars_append["PATH"]

    def test_append_paths_list(self):
        env = wenv.EnvConfig()
        env.append_paths(PATH=["/path1", "/path2"])
        assert "/path1" in env.vars_append["PATH"]
        assert "/path2" in env.vars_append["PATH"]

    def test_append_paths_with_pathsep(self):
        env = wenv.EnvConfig()
        path_str = f"/path1{os.pathsep}/path2"
        env.append_paths(PATH=path_str)
        assert "/path1" in env.vars_append["PATH"]
        assert "/path2" in env.vars_append["PATH"]

    def test_prepend_paths(self):
        env = wenv.EnvConfig()
        env.prepend_paths(PATH="/first/path")
        assert "/first/path" in env.vars_prepend["PATH"]

    def test_set_paths(self):
        env = wenv.EnvConfig()
        env.set_paths(MY_PATH="/custom/path")
        assert "/custom/path" in env.vars_set["MY_PATH"]

    def test_as_string_scalar(self):
        result = wenv.EnvConfig._as_string_("value")
        assert result == "value"

    def test_as_string_list(self):
        result = wenv.EnvConfig._as_string_(["val1", "val2"])
        expected = os.pathsep.join(["val1", "val2"])
        assert result == expected

    def test_render(self):
        env = wenv.EnvConfig(raw_text="echo 'test'", vars_set={"TEST_VAR": "test_value"})
        rendered = env.render()
        assert isinstance(rendered, str)

    def test_copy(self):
        env1 = wenv.EnvConfig(vars_set={"VAR": "value"}, module_load="module1")
        env2 = env1.copy()
        assert env2.vars_set["VAR"] == "value"
        assert env2.module_load == "module1"
        assert env1 is not env2

    def test_module_setup(self):
        env = wenv.EnvConfig(module_setup="source /etc/modules.sh")
        assert env.module_setup == "source /etc/modules.sh"

    def test_conda_setup(self):
        env = wenv.EnvConfig(conda_setup="source /opt/conda/etc/profile.d/conda.sh")
        assert env.conda_setup == "source /opt/conda/etc/profile.d/conda.sh"

    def test_conda_activate(self):
        env = wenv.EnvConfig(conda_activate="myenv")
        assert env.conda_activate == "myenv"

    def test_uv_venv(self):
        env = wenv.EnvConfig(uv_venv="/path/to/venv")
        assert env.uv_venv == "/path/to/venv"

    def test_multiple_append_calls(self):
        env = wenv.EnvConfig()
        env.append_paths(PATH="/path1")
        env.append_paths(PATH="/path2")
        assert "/path1" in env.vars_append["PATH"]
        assert "/path2" in env.vars_append["PATH"]

    def test_vars_forward_preservation(self):
        forward_vars = ["VAR1", "VAR2", "VAR3"]
        env = wenv.EnvConfig(vars_forward=forward_vars)
        assert len(env.vars_forward) == 3
        assert all(v in env.vars_forward for v in forward_vars)
