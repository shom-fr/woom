#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for tasks.py module
"""
import os
from unittest.mock import Mock, patch

import configobj
import pytest

from woom import env as wenv
from woom import hosts as whosts
from woom import tasks as wtasks


class TestTaskTree:
    """Test TaskTree class"""

    def test_init_simple(self):
        stages = configobj.ConfigObj()
        stages["prolog"] = {}
        stages["cycles"] = {}
        stages["epilog"] = {}

        tree = wtasks.TaskTree(stages)
        assert tree is not None

    def test_init_with_groups(self):
        stages = configobj.ConfigObj()
        stages["prolog"] = {"fetch": ["task1", "group1"]}

        groups = configobj.ConfigObj()
        groups["group1"] = ["task2", "task3"]

        tree = wtasks.TaskTree(stages, groups)
        tree_dict = tree.to_dict()
        assert "prolog" in tree_dict

    def test_to_dict_simple(self):
        stages = configobj.ConfigObj()
        stages["prolog"] = {"step1": ["task1"]}

        tree = wtasks.TaskTree(stages)
        result = tree.to_dict()

        assert "prolog" in result
        assert "step1" in result["prolog"]

    def test_to_dict_with_group_expansion(self):
        stages = configobj.ConfigObj()
        stages["prolog"] = {"fetch": ["group1"]}

        groups = configobj.ConfigObj()
        groups["group1"] = ["task1", "task2"]

        tree = wtasks.TaskTree(stages, groups)
        result = tree.to_dict()

        assert result["prolog"]["fetch"][0] == ["task1", "task2"]

    def test_duplicate_task_error(self):
        stages = configobj.ConfigObj()
        stages["prolog"] = {"step1": ["task1"]}
        stages["cycles"] = {"step2": ["task1"]}

        tree = wtasks.TaskTree(stages)
        with pytest.raises(wtasks.TaskError):
            tree.to_dict()

    def test_str_representation(self):
        stages = configobj.ConfigObj()
        stages["prolog"] = {"fetch": ["task1", "task2"]}

        tree = wtasks.TaskTree(stages)
        result = str(tree)

        assert "prolog" in result
        assert "fetch" in result

    def test_empty_workflow(self):
        stages = configobj.ConfigObj()
        stages["prolog"] = {}
        stages["cycles"] = {}
        stages["epilog"] = {}

        tree = wtasks.TaskTree(stages)
        result = str(tree)
        assert "Empty workflow!" in result


class TestTaskManager:
    """Test TaskManager class"""

    def test_init(self):
        host = Mock(spec=whosts.Host)
        manager = wtasks.TaskManager(host)
        assert manager._host == host
        assert isinstance(manager._config, configobj.ConfigObj)

    def test_load_config(self, tmp_path):
        # Create a simple task config
        cfg_file = tmp_path / "tasks.cfg"
        cfg_file.write_text("[task1]\n")

        spec_file = tmp_path / "tasks.ini"
        spec_file.write_text(
            """
[__many__]
    [[content]]
    commandline=string(default=None)
    run_dir=string(default=None)
    env=string(default=None)
    [[artifacts]]
    __many__=string
    [[submit]]
    queue=string(default=None)
        [[[extra]]]
        __many__=string
"""
        )

        host = Mock(spec=whosts.Host)
        manager = wtasks.TaskManager(host)

        # Mock the CFGSPECS_FILE
        with patch.object(wtasks, 'CFGSPECS_FILE', str(spec_file)):
            manager.load_config(str(cfg_file))

        assert "task1" in manager._config

    def test_get_task(self, tmp_path):
        cfg_file = tmp_path / "tasks.cfg"
        cfg_file.write_text(
            """
[task1]
    [[content]]
    commandline = echo test
"""
        )

        spec_file = tmp_path / "tasks.ini"
        spec_file.write_text(
            """
[__many__]
    [[content]]
    commandline=string(default=None)
    run_dir=string(default=None)
    env=string(default=None)
    [[artifacts]]
    __many__=string
    [[submit]]
    queue=string(default=None)
        [[[extra]]]
        __many__=string
"""
        )

        host = Mock(spec=whosts.Host)
        manager = wtasks.TaskManager(host)

        with patch.object(wtasks, 'CFGSPECS_FILE', str(spec_file)):
            manager.load_config(str(cfg_file))

        task = manager.get_task("task1")
        assert isinstance(task, wtasks.Task)
        assert task.name == "task1"

    def test_get_task_invalid(self):
        host = Mock(spec=whosts.Host)
        manager = wtasks.TaskManager(host)

        with pytest.raises(wtasks.TaskError):
            manager.get_task("nonexistent")


class TestTask:
    """Test Task class"""

    def setup_method(self):
        """Setup for each test"""
        self.mock_host = Mock(spec=whosts.Host)
        self.mock_host.get_env.return_value = wenv.EnvConfig()

        self.task_config = configobj.ConfigObj()
        self.task_config.name = "test_task"
        self.task_config["content"] = {
            "commandline": "echo test",
            "run_dir": "/tmp/run",
            "env": None,
        }
        self.task_config["artifacts"] = {}
        self.task_config["submit"] = {
            "queue": None,
            "memory": None,
            "time": None,
            "mail": None,
            "extra": {},
        }

    def test_init(self):
        task = wtasks.Task(self.task_config, self.mock_host)
        assert task.name == "test_task"
        assert task.host == self.mock_host

    def test_name_property(self):
        task = wtasks.Task(self.task_config, self.mock_host)
        assert task.name == "test_task"

    def test_config_property(self):
        task = wtasks.Task(self.task_config, self.mock_host)
        assert task.config == self.task_config

    def test_host_property(self):
        task = wtasks.Task(self.task_config, self.mock_host)
        assert task.host == self.mock_host

    def test_get_run_dir(self):
        task = wtasks.Task(self.task_config, self.mock_host)
        run_dir = task.get_run_dir()
        assert run_dir == "/tmp/run"

    def test_get_run_dir_none(self):
        self.task_config["content"]["run_dir"] = None
        task = wtasks.Task(self.task_config, self.mock_host)
        run_dir = task.get_run_dir()
        assert run_dir == ""

    def test_get_run_dir_current(self):
        self.task_config["content"]["run_dir"] = "current"
        task = wtasks.Task(self.task_config, self.mock_host)
        run_dir = task.get_run_dir()
        assert run_dir == os.getcwd()

    def test_export_commandline(self):
        task = wtasks.Task(self.task_config, self.mock_host)
        cmdline = task.export_commandline()
        assert cmdline == "echo test"

    def test_artifacts_property(self):
        self.task_config["artifacts"] = {"output": "/path/to/output.nc"}
        task = wtasks.Task(self.task_config, self.mock_host)
        assert task.artifacts["output"] == "/path/to/output.nc"

    def test_export_artifacts_checking(self):
        self.task_config["artifacts"] = {"out1": "/file1.txt"}
        task = wtasks.Task(self.task_config, self.mock_host)
        checks = task.export_artifacts_checking()
        assert "test -f" in checks
        assert "/file1.txt" in checks

    def test_export_artifacts_checking_empty(self):
        task = wtasks.Task(self.task_config, self.mock_host)
        checks = task.export_artifacts_checking()
        assert checks == ""

    def test_render_artifacts(self):
        self.task_config["artifacts"] = {"output": "/abs/path/{{ name }}.nc"}
        task = wtasks.Task(self.task_config, self.mock_host)

        params = {"name": "test"}
        artifacts = task.render_artifacts(params)
        assert artifacts["output"] == "/abs/path/test.nc"

    def test_render_artifacts_relative_with_run_dir(self):
        self.task_config["artifacts"] = {"output": "relative/{{ name }}.nc"}
        self.task_config["content"]["run_dir"] = "/run/dir"
        task = wtasks.Task(self.task_config, self.mock_host)

        params = {"name": "test"}
        artifacts = task.render_artifacts(params)
        assert artifacts["output"] == "/run/dir/relative/test.nc"

    def test_render_artifacts_relative_no_run_dir_error(self):
        self.task_config["artifacts"] = {"output": "relative.nc"}
        self.task_config["content"]["run_dir"] = None
        task = wtasks.Task(self.task_config, self.mock_host)

        with pytest.raises(wtasks.TaskError):
            task.render_artifacts({})

    def test_export_scheduler_options(self):
        self.mock_host.__getitem__ = Mock(return_value="slurm")
        self.mock_host.__getitem__.side_effect = lambda x: {
            "scheduler": "slurm",
            "queues": {"seq": "normal"},
        }.get(x, {})

        self.task_config["submit"]["queue"] = "seq"
        self.task_config["submit"]["memory"] = "4GB"
        self.task_config["submit"]["time"] = "01:00:00"

        task = wtasks.Task(self.task_config, self.mock_host)
        opts = task.export_scheduler_options()

        assert opts["memory"] == "4GB"
        assert opts["time"] == "01:00:00"
