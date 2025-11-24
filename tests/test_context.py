#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for woom.context module

Place this file at the root of your project (same level as woom/ directory)
Run: pytest test_context.py -v
"""
import os
from unittest.mock import MagicMock, Mock

import pytest
from configobj import ConfigObj

from woom.context import Context
from woom.iters import Cycle, Member


@pytest.fixture
def mock_workflow():
    """Create a minimal mock workflow"""
    workflow = Mock()
    workflow.config = ConfigObj(
        {
            'params': {'hosts': {}, 'tasks': {}},
            'env_vars': {},
            'app': {'name': 'test', 'conf': 'conf1', 'exp': 'exp1'},
            'cycles': {'begin_date': '2020-01-01'},
        }
    )
    workflow.host = Mock()
    workflow.host.name = 'local'
    workflow.host.get_params = Mock(return_value={'scratch_dir': '/tmp/scratch'})
    workflow.taskmanager = Mock()
    workflow.jobmanager = Mock()
    workflow.logger = Mock()
    workflow.task_tree = {}
    workflow.cycles = []
    workflow.nmembers = 0
    workflow.members = []
    workflow.paths = {'PATH': '/usr/bin'}
    workflow.workflow_dir = '/tmp/workflow'
    workflow.get_app_path = Mock(return_value='test/conf1/exp1')
    workflow.get_task = Mock()
    workflow.get_task_path = Mock(return_value='test/task1')
    workflow.get_task_submission_dir = Mock(return_value='/tmp/workflow/jobs/task1')
    return workflow


def test_context_init(mock_workflow):
    """Test basic Context initialization"""
    context = Context(mock_workflow)

    assert context['workflow'] is mock_workflow
    assert context['host'] is mock_workflow.host
    assert context['config'] is mock_workflow.config
    assert 'params' in context
    assert 'env_vars' in context


def test_context_with_task(mock_workflow):
    """Test Context with task_name"""
    mock_task = Mock()
    mock_task.name = 'task1'
    mock_task.run_dir = '/tmp/run'
    mock_task.env = Mock()
    mock_task.env.prepend_paths = Mock()
    mock_workflow.get_task.return_value = mock_task

    context = Context(mock_workflow, task_name='task1')

    assert context['task'] is mock_task
    assert context['task_name'] == 'task1'
    mock_workflow.get_task.assert_called_once_with('task1')


def test_context_properties(mock_workflow):
    """Test Context properties"""
    context = Context(mock_workflow)

    assert context.workflow is mock_workflow
    assert context.config is mock_workflow.config
    assert isinstance(context.env_vars, dict)
    assert context.task is None
    assert context.cycle is None
    assert context.member is None


def test_context_set_params(mock_workflow):
    """Test set_params method"""
    mock_task = Mock()
    mock_task.env = Mock()
    mock_task.env.vars_set = {}
    mock_workflow.get_task.return_value = mock_task

    context = Context(mock_workflow, task_name='task1')

    # Add some parameters
    context.set_params({'test_param': 'test_value', 'number': 42})

    assert context['test_param'] == 'test_value'
    assert context['number'] == 42
    assert 'WOOM_TEST_PARAM' in context['env_vars']
    assert context['env_vars']['WOOM_TEST_PARAM'] == 'test_value'


def test_context_manager(mock_workflow):
    """Test Context as context manager"""
    mock_task = Mock()
    mock_task.env = Mock()
    mock_task.env.prepend_paths = Mock()
    mock_workflow.get_task.return_value = mock_task

    context = Context(mock_workflow, task_name='task1')

    with context as ctx:
        assert mock_workflow.context is ctx
        assert mock_task.context is ctx

    assert not hasattr(mock_workflow, 'context')
    assert not hasattr(mock_task, 'context')


def test_context_with_cycle(mock_workflow):
    """Test Context with cycle"""
    cycle = Cycle('2020-01-01', '2020-01-02')

    context = Context(mock_workflow, cycle=cycle)

    assert context.cycle is cycle
    assert context['cycle'] is cycle
    assert 'cycle_begin_date' in context['params']


def test_context_with_member(mock_workflow):
    """Test Context with member"""
    member = Member(1, 10)

    context = Context(mock_workflow, member=member)

    assert context.member is member
    assert context['member'] is member


def test_context_copy(mock_workflow):
    """Test Context copy method"""
    context = Context(mock_workflow)
    context_copy = context.copy()

    assert isinstance(context_copy, Context)
    assert context_copy.workflow is mock_workflow
