#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for woom.tasks module

Place this file at the root of your project (same level as woom/ directory)
Run: pytest test_tasks.py -v
"""
import os
from unittest.mock import MagicMock, Mock, patch

import pytest
from configobj import ConfigObj

from woom.tasks import ARTIFACTS_GENERATORS, Task, TaskError, TaskManager, TaskTree


@pytest.fixture
def sample_stages():
    """Create sample stages configuration"""
    return ConfigObj(
        {
            'prolog': {'setup': ['task1', 'task2']},
            'cycles': {'process': ['task3', 'task4']},
            'epilog': {'cleanup': ['task5']},
        }
    )


@pytest.fixture
def sample_groups():
    """Create sample groups configuration"""
    return ConfigObj(
        {
            'group1': ['task1', 'task2'],
        }
    )


@pytest.fixture
def mock_host():
    """Create a mock host"""
    host = Mock()
    host.name = 'local'
    host.get_env = Mock()
    mock_env = Mock()
    mock_env.vars_set = {}
    mock_env.copy = Mock(return_value=mock_env)
    host.get_env.return_value = mock_env
    host.config = {'scheduler': 'background', 'queues': {'seq': 'sequential'}}
    return host


@pytest.fixture
def sample_task_config():
    """Create a sample task configuration"""
    return ConfigObj(
        {
            'task1': {
                'content': {
                    'commandline': 'echo "test"',
                    'run_dir': '/tmp/run',
                    'env': None,
                },
                'artifacts': {
                    'output': {
                        'path': ['/tmp/output.txt'],
                        'check': True,
                        'callable': False,
                        'kwargs': {},
                    }
                },
                'submit': {
                    'queue': 'seq',
                    'nnodes': None,
                    'ncpus': None,
                    'ngpus': None,
                    'memory': None,
                    'pmem': None,
                    'time': None,
                    'mail': None,
                },
            }
        }
    )


class TestTaskTree:
    """Test TaskTree class"""

    def test_init(self, sample_stages, sample_groups):
        """Test TaskTree initialization"""
        tree = TaskTree(sample_stages, sample_groups)
        assert tree._stages == sample_stages
        assert tree._groups == sample_groups

    def test_to_dict_basic(self, sample_stages):
        """Test to_dict with basic stages"""
        tree = TaskTree(sample_stages, None)
        result = tree.to_dict()

        assert 'prolog' in result
        assert 'cycles' in result
        assert 'epilog' in result
        assert result['prolog']['setup'] == [['task1'], ['task2']]
        assert result['cycles']['process'] == [['task3'], ['task4']]

    def test_to_dict_with_groups(self, sample_groups):
        """Test to_dict with groups"""
        stages = ConfigObj({'prolog': {'init': ['group1']}})
        tree = TaskTree(stages, sample_groups)
        result = tree.to_dict()

        assert result['prolog']['init'] == [['task1', 'task2']]

    def test_str_representation(self, sample_stages):
        """Test string representation"""
        tree = TaskTree(sample_stages, None)
        tree_str = str(tree)

        assert 'prolog' in tree_str
        assert 'task1' in tree_str

    def test_get_task_stage(self, sample_stages):
        """Test get_task_stage method"""
        tree = TaskTree(sample_stages, None)

        assert tree.get_task_stage('task1') == 'prolog'
        assert tree.get_task_stage('task3') == 'cycles'
        assert tree.get_task_stage('task5') == 'epilog'

    def test_duplicate_tasks_error(self):
        """Test error on duplicate tasks"""
        stages = ConfigObj(
            {
                'prolog': {'init': ['task1']},
                'cycles': {'process': ['task1']},  # Duplicate
            }
        )
        tree = TaskTree(stages, None)

        with pytest.raises(TaskError, match="Duplicate tasks not allowed"):
            tree.to_dict()


class TestTaskManager:
    """Test TaskManager class"""

    def test_init(self, mock_host):
        """Test TaskManager initialization"""
        manager = TaskManager(mock_host)

        assert manager._host is mock_host
        assert isinstance(manager._config, ConfigObj)
        assert manager._configs == []

    @patch('woom.tasks.wconf.load_cfg')
    def test_load_config(self, mock_load_cfg, mock_host, sample_task_config):
        """Test load_config method"""
        mock_load_cfg.return_value = sample_task_config
        manager = TaskManager(mock_host)

        manager.load_config('tasks.cfg')

        assert len(manager._configs) == 1

    def test_get_task(self, mock_host, sample_task_config):
        """Test get_task method"""
        manager = TaskManager(mock_host)
        manager._config = sample_task_config

        task = manager.get_task('task1')

        assert isinstance(task, Task)
        assert task.name == 'task1'

    def test_get_task_invalid(self, mock_host):
        """Test get_task with invalid name"""
        manager = TaskManager(mock_host)
        manager._config = ConfigObj()

        with pytest.raises(TaskError, match="Invalid task name"):
            manager.get_task('nonexistent')

    def test_host_property(self, mock_host):
        """Test host property"""
        manager = TaskManager(mock_host)
        assert manager.host is mock_host


class TestTask:
    """Test Task class"""

    def test_init(self, mock_host):
        """Test Task initialization"""
        config = ConfigObj(
            {
                'content': {'commandline': 'test', 'run_dir': '/tmp', 'env': None},
                'artifacts': {},
                'submit': {},
            }
        )
        task = Task(config, mock_host)

        assert task._config is config
        assert task._host is mock_host

    def test_properties(self, mock_host):
        """Test Task properties"""
        config = ConfigObj(
            {
                'content': {'commandline': 'test', 'run_dir': '/tmp', 'env': None},
                'artifacts': {},
                'submit': {},
            }
        )
        config.name = 'task1'
        task = Task(config, mock_host)

        assert task.config is config
        assert task.host is mock_host
        assert task.name == 'task1'

    def test_run_dir(self, mock_host):
        """Test run_dir property"""
        config = ConfigObj(
            {
                'content': {'run_dir': '/tmp/test', 'commandline': '', 'env': None},
                'artifacts': {},
                'submit': {},
            }
        )
        task = Task(config, mock_host)

        assert task.run_dir == '/tmp/test'

    def test_run_dir_current(self, mock_host):
        """Test run_dir with 'current' value"""
        config = ConfigObj(
            {
                'content': {'run_dir': 'current', 'commandline': '', 'env': None},
                'artifacts': {},
                'submit': {},
            }
        )
        task = Task(config, mock_host)

        assert task.run_dir == os.getcwd()

    def test_commandline(self, mock_host):
        """Test commandline property"""
        config = ConfigObj(
            {
                'content': {'commandline': 'echo "hello"', 'run_dir': '', 'env': None},
                'artifacts': {},
                'submit': {},
            }
        )
        task = Task(config, mock_host)

        assert task.commandline == 'echo "hello"'

    def test_get_artifacts(self, mock_host):
        """Test get_artifacts method"""
        config = ConfigObj(
            {
                'content': {'commandline': '', 'run_dir': '/tmp', 'env': None},
                'artifacts': {
                    'output': {
                        'path': ['/tmp/output.txt'],
                        'check': True,
                        'callable': False,
                        'kwargs': {},
                    }
                },
                'submit': {},
            }
        )
        task = Task(config, mock_host)

        artifacts = task.get_artifacts()

        assert 'output' in artifacts
        assert artifacts['output'] == ['/tmp/output.txt']

    def test_context_setter(self, mock_host):
        """Test context setter"""
        config = ConfigObj(
            {
                'content': {'commandline': '', 'run_dir': '', 'env': None},
                'artifacts': {},
                'submit': {},
            }
        )
        task = Task(config, mock_host)
        mock_context = {'task': None}

        task.context = mock_context

        assert task._context is mock_context
        assert mock_context['task'] is task

    def test_export_scheduler_options(self, mock_host):
        """Test export_scheduler_options method"""
        config = ConfigObj(
            {
                'content': {'commandline': '', 'run_dir': '', 'env': None},
                'artifacts': {},
                'submit': {
                    'queue': 'seq',
                    'nnodes': 2,
                    'ncpus': 4,
                    'ngpus': None,
                    'memory': '8GB',
                    'pmem': None,
                    'time': '01:00:00',
                    'mail': None,
                },
            }
        )
        task = Task(config, mock_host)

        opts = task.export_scheduler_options()

        assert opts['nnodes'] == 2
        assert opts['ncpus'] == 4
        assert opts['memory'] == '8GB'
        assert opts['queue'] == 'sequential'

    def test_is_blocking_true(self, mock_host):
        """Test is_blocking property returns True"""
        config = ConfigObj(
            {
                'content': {'commandline': '', 'run_dir': '', 'env': None},
                'artifacts': {},
                'submit': {'blocking': True},
            }
        )
        task = Task(config, mock_host)

        assert task.is_blocking is True

    def test_is_blocking_false(self, mock_host):
        """Test is_blocking property returns False"""
        config = ConfigObj(
            {
                'content': {'commandline': '', 'run_dir': '', 'env': None},
                'artifacts': {},
                'submit': {'blocking': False},
            }
        )
        task = Task(config, mock_host)

        assert task.is_blocking is False

    def test_template_config(self, mock_host):
        """Test template configuration is accessible"""
        config = ConfigObj(
            {
                'content': {
                    'commandline': 'echo test',
                    'run_dir': '/tmp',
                    'env': None,
                    'template': 'custom.sh',
                },
                'artifacts': {},
                'submit': {},
            }
        )
        task = Task(config, mock_host)

        assert task.config['content']['template'] == 'custom.sh'
