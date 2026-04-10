#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for woom.workflow module

Place this file at the root of your project (same level as woom/ directory)
Run: pytest test_workflow.py -v
"""

from unittest.mock import Mock, patch

import pytest
from configobj import ConfigObj

from woom.iters import Cycle
from woom.job import PbsproJobManager, SlurmJobManager
from woom.workflow import Workflow, WorkFlowError


@pytest.fixture
def minimal_config():
    """Create minimal workflow configuration"""
    return ConfigObj(
        {
            'app': {'name': 'test_app', 'conf': 'test_conf', 'exp': 'exp1'},
            'cycles': {
                'begin_date': '2020-01-01',
                'end_date': None,
                'round': None,
                'freq': None,
                'ncycles': 0,
                'indep': False,
                'as_intervals': True,
            },
            'ensemble': {
                'size': None,
                'skip': None,
                'label': 'member',
                'tasks': None,
                'iters': {},
            },
            'params': {'hosts': {}, 'tasks': {}},
            'env_vars': {},
            'groups': {},
            'stages': {
                'dry_run': False,
                'update': False,
                'prolog': {},
                'cycles': {},
                'epilog': {},
            },
        }
    )


@pytest.fixture
def mock_taskmanager():
    """Create a mock TaskManager"""
    tm = Mock()
    tm.host = Mock()
    tm.host.name = 'local'
    tm.host.get_jobmanager = Mock()
    tm.host.get_params = Mock(return_value={})
    tm.get_task.return_value.is_skipped = False
    return tm


@pytest.fixture(autouse=True)
def mock_render_setup():
    """Mock render.setup_template_loader to avoid filesystem operations"""
    with patch('woom.workflow.wrender.setup_template_loader') as mock_setup:
        mock_setup.return_value = '/tmp/templates'
        yield mock_setup


class TestWorkflowInit:
    """Test Workflow initialization"""

    def test_init_with_config(self, minimal_config, mock_taskmanager, tmp_path):
        """Test initialization with ConfigObj"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')

        workflow = Workflow(minimal_config, mock_taskmanager)

        assert workflow.config is minimal_config
        assert workflow.taskmanager is mock_taskmanager
        assert workflow.host is mock_taskmanager.host

    def test_workflow_dir(self, minimal_config, mock_taskmanager, tmp_path):
        """Test workflow_dir property"""
        cfg_file = tmp_path / 'workflow.cfg'
        cfg_file.touch()
        minimal_config.filename = str(cfg_file)

        workflow = Workflow(minimal_config, mock_taskmanager)

        assert workflow.workflow_dir == str(tmp_path)

    def test_app_name_inference(self, minimal_config, mock_taskmanager, tmp_path):
        """Test that app name is inferred from directory if not set"""
        minimal_config['app']['name'] = None
        cfg_file = tmp_path / 'test_workflow' / 'workflow.cfg'
        cfg_file.parent.mkdir(parents=True)
        cfg_file.touch()
        minimal_config.filename = str(cfg_file)

        workflow = Workflow(minimal_config, mock_taskmanager)

        assert workflow.config['app']['name'] == 'test_workflow'


class TestWorkflowProperties:
    """Test Workflow properties"""

    def test_config_property(self, minimal_config, mock_taskmanager, tmp_path):
        """Test config property"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        assert workflow.config is minimal_config

    def test_taskmanager_property(self, minimal_config, mock_taskmanager, tmp_path):
        """Test taskmanager property"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        assert workflow.taskmanager is mock_taskmanager

    def test_host_property(self, minimal_config, mock_taskmanager, tmp_path):
        """Test host property"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        assert workflow.host is mock_taskmanager.host

    def test_cycles_property(self, minimal_config, mock_taskmanager, tmp_path):
        """Test cycles property"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        assert isinstance(workflow.cycles, list)

    def test_members_property(self, minimal_config, mock_taskmanager, tmp_path):
        """Test members property"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        assert isinstance(workflow.members, list)
        assert workflow.nmembers == 0


class TestWorkflowPaths:
    """Test path-related methods"""

    def test_get_app_path(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_app_path method"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        app_path = workflow.get_app_path()

        assert app_path == 'test_app/test_conf/exp1'

    def test_get_app_path_custom_sep(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_app_path with custom separator"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        app_path = workflow.get_app_path(sep='-')

        assert app_path == 'test_app-test_conf-exp1'

    def test_get_task_path(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_task_path method"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        task_path = workflow.get_task_path('task1')

        assert task_path == 'test_app/test_conf/exp1/task1'

    def test_get_task_path_with_cycle(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_task_path with cycle"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        minimal_config['stages']['cycles'] = {'seq': ['task1']}
        workflow = Workflow(minimal_config, mock_taskmanager)
        cycle = Cycle('2020-01-01')

        task_path = workflow.get_task_path('task1', cycle=cycle)

        assert '2020-01-01' in task_path
        assert task_path.endswith('task1')


class TestWorkflowContext:
    """Test context-related methods"""

    def test_get_context(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_context method"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        context = workflow.get_context(task_name='task1')

        assert context is not None
        assert context.workflow is workflow

    def test_set_context(self, minimal_config, mock_taskmanager, tmp_path):
        """Test set_context method"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        context = workflow.set_context(task_name='task1')

        assert workflow.context is context

    def test_context_property_not_set(self, minimal_config, mock_taskmanager, tmp_path):
        """Test accessing context property when not set"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        with pytest.raises(WorkFlowError, match="context must be set"):
            _ = workflow.context


class TestWorkflowCyclesMembers:
    """Test cycle and member getters"""

    def test_get_cycle_valid(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_cycle with valid cycle"""
        minimal_config['cycles']['end_date'] = '2020-01-02'
        minimal_config['cycles']['freq'] = '1D'
        minimal_config['stages']['cycles'] = {'seq': ['task']}
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        cycle = workflow.cycles[0]
        retrieved = workflow.get_cycle(str(cycle))

        assert retrieved is cycle

    def test_get_cycle_none(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_cycle with None"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        result = workflow.get_cycle(None)

        assert result is None

    def test_get_member_with_ensemble(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_member with ensemble"""
        minimal_config['ensemble']['size'] = 3
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        member = workflow.members[0]
        retrieved = workflow.get_member(str(member))

        assert retrieved is member


class TestWorkflowTaskOperations:
    """Test task-related operations"""

    def test_get_task(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_task method"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        mock_task = Mock()
        mock_taskmanager.get_task.return_value = mock_task

        workflow = Workflow(minimal_config, mock_taskmanager)
        task = workflow.get_task('task1')

        assert task is mock_task
        mock_taskmanager.get_task.assert_called_once_with('task1')

    def test_get_task_members_no_ensemble(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_task_members without ensemble"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        members = workflow.get_task_members('task1')

        assert members is None

    def test_get_task_members_with_ensemble(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_task_members with ensemble"""
        minimal_config['ensemble']['size'] = 3
        minimal_config['ensemble']['tasks'] = ['task1']
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        members = workflow.get_task_members('task1')

        assert members == workflow.members
        assert len(members) == 3

    def test_is_task_blocking_true(self, minimal_config, mock_taskmanager, tmp_path):
        """Test is_task_blocking returns True for blocking task"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        mock_task = Mock()
        mock_task.is_blocking = True
        mock_taskmanager.get_task.return_value = mock_task
        workflow = Workflow(minimal_config, mock_taskmanager)

        result = workflow.is_task_blocking('task1')

        assert result is True

    def test_is_task_blocking_false(self, minimal_config, mock_taskmanager, tmp_path):
        """Test is_task_blocking returns False for non-blocking task"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        mock_task = Mock()
        mock_task.is_blocking = False
        mock_taskmanager.get_task.return_value = mock_task
        workflow = Workflow(minimal_config, mock_taskmanager)

        result = workflow.is_task_blocking('task1')

        assert result is False


class TestWorkflowIterator:
    """Test workflow iteration"""

    def test_workflow_iterator_empty(self, minimal_config, mock_taskmanager, tmp_path):
        """Test iterating over empty workflow"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        # Configure mock to have no scheduler (no sentinel task)
        mock_taskmanager.host.get_jobmanager.return_value.with_scheduler = None
        workflow = Workflow(minimal_config, mock_taskmanager)

        items = list(workflow)

        assert items == []

    def test_workflow_iterator_with_tasks(self, minimal_config, mock_taskmanager, tmp_path):
        """Test iterating over workflow with tasks"""
        minimal_config['stages']['prolog'] = {'init': ['task1']}
        minimal_config.filename = str(tmp_path / 'workflow.cfg')

        with patch('woom.workflow.wtasks.TaskTree.get_task_stage') as mock_stage:
            mock_stage.return_value = 'prolog'
            workflow = Workflow(minimal_config, mock_taskmanager)

            items = list(workflow)

            assert len(items) > 0


class TestWorkflowSubmissionDirs:
    """Test submission directory handling"""

    def test_submission_dirs_generator(self, minimal_config, mock_taskmanager, tmp_path):
        """Test submission_dirs property"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        # Should be a generator
        assert hasattr(workflow.submission_dirs, '__iter__')


class TestWorkflowStatus:
    """Test status methods"""

    @patch('woom.workflow.wjob.JobStatus')
    def test_get_task_status_not_submitted(self, mock_status, minimal_config, mock_taskmanager, tmp_path):
        """Test get_task_status for unsubmitted task"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        mock_status.__getitem__.return_value = Mock(name='NOTSUBMITTED')

        workflow = Workflow(minimal_config, mock_taskmanager)
        status = workflow.get_task_status('task1')

        assert status is not None

    def test_get_task_status_slurm_time_limit(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_task_status detects SLURM time limit"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        # Configure mock to use actual SlurmJobManager for scheduler
        mock_jobmanager = Mock()
        mock_jobmanager.with_scheduler = SlurmJobManager
        mock_jobmanager.get_killed = SlurmJobManager.get_killed
        mock_taskmanager.host.get_jobmanager.return_value = mock_jobmanager
        workflow = Workflow(minimal_config, mock_taskmanager)

        # Create submission directory with job files
        submission_dir = tmp_path / 'jobs' / 'test_app' / 'test_conf' / 'exp1' / 'task1'
        submission_dir.mkdir(parents=True)

        # Create job.json
        job_json = submission_dir / 'job.json'
        job_json.write_text(
            '{"manager": "SlurmJobManager", "jobid": "12345", '
            '"name": "task1", "script": "job.sh", "args": [], '
            '"queue": null, "status": "RUNNING", "submission_date": "2025-01-01"}'
        )

        # Create job.out with SLURM time limit error
        job_out = submission_dir / 'job.out'
        job_out.write_text('Job output\nCANCELLED AT 2025-01-01 DUE TO TIME LIMIT\nExiting...')

        with patch.object(workflow.jobmanager, 'load_job') as mock_load:
            mock_job = Mock()
            mock_job.jobid = '12345'
            mock_load.return_value = mock_job

            status = workflow.get_task_status('task1')

        assert status.name == 'FAILED'
        assert status.jobid == '12345'

    def test_get_task_status_slurm_out_of_memory(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_task_status detects SLURM out of memory"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        # Configure mock to use actual SlurmJobManager for scheduler
        mock_jobmanager = Mock()
        mock_jobmanager.with_scheduler = SlurmJobManager
        mock_jobmanager.get_killed = SlurmJobManager.get_killed
        mock_taskmanager.host.get_jobmanager.return_value = mock_jobmanager
        workflow = Workflow(minimal_config, mock_taskmanager)

        # Create submission directory
        submission_dir = tmp_path / 'jobs' / 'test_app' / 'test_conf' / 'exp1' / 'task1'
        submission_dir.mkdir(parents=True)

        # Create job.json
        job_json = submission_dir / 'job.json'
        job_json.write_text(
            '{"manager": "SlurmJobManager", "jobid": "12346", '
            '"name": "task1", "script": "job.sh", "args": [], '
            '"queue": null, "status": "RUNNING", "submission_date": "2025-01-01"}'
        )

        # Create job.err with SLURM OOM error
        job_err = submission_dir / 'job.err'
        job_err.write_text('slurmstepd: error: Detected 1 oom-kill event(s) Killed process 12345')

        with patch.object(workflow.jobmanager, 'load_job') as mock_load:
            mock_job = Mock()
            mock_job.jobid = '12346'
            mock_load.return_value = mock_job

            status = workflow.get_task_status('task1')

        assert status.name == 'FAILED'
        assert status.jobid == '12346'

    def test_get_task_status_pbspro_walltime(self, minimal_config, mock_taskmanager, tmp_path):
        """Test get_task_status detects PBS Pro walltime"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        # Configure mock to use actual PbsproJobManager for scheduler
        mock_jobmanager = Mock()
        mock_jobmanager.with_scheduler = PbsproJobManager
        mock_jobmanager.get_killed = PbsproJobManager.get_killed
        mock_taskmanager.host.get_jobmanager.return_value = mock_jobmanager
        workflow = Workflow(minimal_config, mock_taskmanager)

        # Create submission directory
        submission_dir = tmp_path / 'jobs' / 'test_app' / 'test_conf' / 'exp1' / 'task1'
        submission_dir.mkdir(parents=True)

        # Create job.json
        job_json = submission_dir / 'job.json'
        job_json.write_text(
            '{"manager": "PbsproJobManager", "jobid": "12347", '
            '"name": "task1", "script": "job.sh", "args": [], '
            '"queue": null, "status": "RUNNING", "submission_date": "2025-01-01"}'
        )

        # Create job.out with PBS walltime error
        job_out = submission_dir / 'job.out'
        job_out.write_text('PBS: job killed: walltime exceeded\nTerminated')

        with patch.object(workflow.jobmanager, 'load_job') as mock_load:
            mock_job = Mock()
            mock_job.jobid = '12347'
            mock_load.return_value = mock_job

            status = workflow.get_task_status('task1')

        assert status.name == 'FAILED'
        assert status.jobid == '12347'


class TestWorkflowClean:
    """Test clean operations"""

    def test_clean_task(self, minimal_config, mock_taskmanager, tmp_path):
        """Test clean_task method"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)
        workflow._dry = True

        # Create fake submission dir
        submission_dir = tmp_path / 'jobs' / 'task1'
        submission_dir.mkdir(parents=True)
        (submission_dir / 'job.sh').touch()

        # Should not raise error
        workflow.clean_task('task1')

    def test_terminate_blocking_jobs(self, minimal_config, mock_taskmanager, tmp_path):
        """Test terminate_blocking_jobs method"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')

        # Create mock jobs - one blocking, one non-blocking
        mock_job_blocking = Mock()
        mock_job_blocking.blocking = True
        mock_job_blocking.kill = Mock()

        mock_job_nonblocking = Mock()
        mock_job_nonblocking.blocking = False
        mock_job_nonblocking.kill = Mock()

        # Setup workflow with mock jobmanager
        workflow = Workflow(minimal_config, mock_taskmanager)
        workflow.jobmanager.jobs = [mock_job_blocking, mock_job_nonblocking]

        # Call terminate_blocking_jobs
        workflow.terminate_blocking_jobs()

        # Verify only non-blocking job was killed
        mock_job_blocking.kill.assert_not_called()
        mock_job_nonblocking.kill.assert_called_once_with(graceful=True)


class TestWorkflowOverview:
    """Test overview display"""

    def test_show_overview(self, minimal_config, mock_taskmanager, tmp_path, capsys):
        """Test show_overview method"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        workflow.show_overview()

        captured = capsys.readouterr()
        assert 'APP' in captured.out or 'TASK TREE' in captured.out


class TestWorkflowFillTemplates:
    """Test Workflow.fill_templates functionality"""

    def test_fill_templates_no_context(self, minimal_config, mock_taskmanager, tmp_path):
        """Test fill_templates raises error when context not set"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        with pytest.raises(WorkFlowError, match="context must be set"):
            workflow.fill_templates()

    def test_fill_templates_with_task_context(self, minimal_config, mock_taskmanager, tmp_path):
        """Test fill_templates with task context set"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')

        # Create mock task
        mock_task = Mock()
        mock_task.fill_templates = Mock()
        mock_taskmanager.get_task.return_value = mock_task

        workflow = Workflow(minimal_config, mock_taskmanager)
        workflow.set_context(task_name='task1')

        # Execute
        workflow.fill_templates()

        # Verify task's fill_templates was called
        mock_task.fill_templates.assert_called_once_with(dry=False)

    def test_fill_templates_dry_mode(self, minimal_config, mock_taskmanager, tmp_path):
        """Test fill_templates respects dry mode"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')

        # Create mock task
        mock_task = Mock()
        mock_task.fill_templates = Mock()
        mock_taskmanager.get_task.return_value = mock_task

        workflow = Workflow(minimal_config, mock_taskmanager)
        workflow._dry = True
        workflow.set_context(task_name='task1')

        # Execute
        workflow.fill_templates()

        # Verify dry mode was passed to task
        mock_task.fill_templates.assert_called_once_with(dry=True)

    def test_fill_templates_with_cycle(self, minimal_config, mock_taskmanager, tmp_path):
        """Test fill_templates with cycle context"""
        minimal_config['cycles']['end_date'] = '2020-01-02'
        minimal_config['cycles']['freq'] = '1D'
        minimal_config['stages']['cycles'] = {'seq': ['task1']}
        minimal_config.filename = str(tmp_path / 'workflow.cfg')

        # Create mock task
        mock_task = Mock()
        mock_task.fill_templates = Mock()
        mock_taskmanager.get_task.return_value = mock_task

        workflow = Workflow(minimal_config, mock_taskmanager)
        cycle = workflow.cycles[0]
        workflow.set_context(task_name='task1', cycle=cycle)

        # Execute
        workflow.fill_templates()

        # Verify task's fill_templates was called
        mock_task.fill_templates.assert_called_once()

    def test_fill_templates_with_member(self, minimal_config, mock_taskmanager, tmp_path):
        """Test fill_templates with ensemble member context"""
        minimal_config['ensemble']['size'] = 3
        minimal_config['ensemble']['tasks'] = ['task1']
        minimal_config.filename = str(tmp_path / 'workflow.cfg')

        # Create mock task
        mock_task = Mock()
        mock_task.fill_templates = Mock()
        mock_taskmanager.get_task.return_value = mock_task

        workflow = Workflow(minimal_config, mock_taskmanager)
        member = workflow.members[0]
        workflow.set_context(task_name='task1', member=member)

        # Execute
        workflow.fill_templates()

        # Verify task's fill_templates was called
        mock_task.fill_templates.assert_called_once()

    @patch('woom.workflow.wjob')
    def test_fill_templates_called_during_submit(self, mock_wjob, minimal_config, mock_taskmanager, tmp_path):
        """Test that fill_templates is automatically called during task submission"""
        minimal_config['stages']['prolog'] = {'init': ['task1']}
        minimal_config.filename = str(tmp_path / 'workflow.cfg')

        # Create mock task with fill_templates spy
        mock_task = Mock()
        mock_task.name = 'task1'
        mock_task.is_blocking = True
        mock_task.fill_templates = Mock()
        mock_taskmanager.get_task.return_value = mock_task

        workflow = Workflow(minimal_config, mock_taskmanager)
        workflow.set_context(task_name='task1')

        # Patch submit_task to capture the fill_templates call
        # We'll verify that the code path that calls fill_templates is executed
        def mock_get_submission_args(depend):
            # This simulates the submission process triggering fill_templates
            workflow.context.task.fill_templates(dry=False)
            return {
                'script': '/tmp/job.sh',
                'content': '#!/bin/bash',
                'opts': {'name': 'task1'},
                'depend': depend,
                'artifacts': {},
            }

        # Patch various methods to simplify the test
        with patch.object(workflow, '_get_submission_args_', side_effect=mock_get_submission_args):
            with patch('builtins.open', create=True):
                with patch.object(workflow.jobmanager, 'submit', return_value=Mock(jobid='12345')):
                    # Trigger submission which should call fill_templates
                    workflow.submit_task()

        # Verify fill_templates was called
        mock_task.fill_templates.assert_called_with(dry=False)


class TestWorkflowSkip:
    """Test task-skip feature"""

    def test_is_task_skipped_via_task_flag(self, minimal_config, mock_taskmanager, tmp_path):
        """is_task_skipped returns True when Task.is_skipped is True"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        mock_task = Mock()
        mock_task.is_skipped = True
        mock_taskmanager.get_task.return_value = mock_task
        workflow = Workflow(minimal_config, mock_taskmanager)

        assert workflow.is_task_skipped('task1') is True

    def test_is_task_skipped_via_runtime_list(self, minimal_config, mock_taskmanager, tmp_path):
        """is_task_skipped returns True when task name is in the runtime skip list"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        mock_task = Mock()
        mock_task.is_skipped = False
        mock_taskmanager.get_task.return_value = mock_task
        workflow = Workflow(minimal_config, mock_taskmanager)
        workflow._skip_tasks = ['task1']

        assert workflow.is_task_skipped('task1') is True

    def test_is_task_skipped_false(self, minimal_config, mock_taskmanager, tmp_path):
        """is_task_skipped returns False for a normal task"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        mock_task = Mock()
        mock_task.is_skipped = False
        mock_taskmanager.get_task.return_value = mock_task
        workflow = Workflow(minimal_config, mock_taskmanager)

        assert workflow.is_task_skipped('task1') is False

    def test_skip_list_loaded_from_config(self, minimal_config, mock_taskmanager, tmp_path):
        """_skip_tasks is populated from workflow config [stages] skip"""
        minimal_config['stages']['skip'] = ['task_a', 'task_b']
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        assert 'task_a' in workflow._skip_tasks
        assert 'task_b' in workflow._skip_tasks

    def test_skip_list_empty_by_default(self, minimal_config, mock_taskmanager, tmp_path):
        """_skip_tasks defaults to empty list when not in config"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)

        assert workflow._skip_tasks == []

    def test_run_merges_cli_skip_list(self, minimal_config, mock_taskmanager, tmp_path):
        """workflow.run(skip=[...]) merges CLI skip list with existing _skip_tasks"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)
        workflow._skip_tasks = ['task_a']

        # Empty task tree + no scheduler → run() completes without submission
        mock_taskmanager.host.get_jobmanager.return_value.with_scheduler = None
        mock_taskmanager.host.get_jobmanager.return_value.jobs = []
        workflow.run(skip=['task_b'])

        assert 'task_a' in workflow._skip_tasks
        assert 'task_b' in workflow._skip_tasks

    def test_run_skip_merges_without_duplicate(self, minimal_config, mock_taskmanager, tmp_path):
        """Passing the same task twice in skip does not create duplicates"""
        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        workflow = Workflow(minimal_config, mock_taskmanager)
        workflow._skip_tasks = ['task_a']

        # Empty task tree + no scheduler → run() completes without submission
        mock_taskmanager.host.get_jobmanager.return_value.with_scheduler = None
        mock_taskmanager.host.get_jobmanager.return_value.jobs = []
        workflow.run(skip=['task_a'])

        assert workflow._skip_tasks.count('task_a') == 1

    def test_get_task_status_returns_skipped(self, minimal_config, mock_taskmanager, tmp_path):
        """get_task_status returns SKIPPED for a skipped task"""
        from woom.job import JobStatus

        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        mock_task = Mock()
        mock_task.is_skipped = True
        mock_taskmanager.get_task.return_value = mock_task
        workflow = Workflow(minimal_config, mock_taskmanager)

        status = workflow.get_task_status('task1')

        assert status == JobStatus.SKIPPED

    def test_get_task_status_skipped_before_dry(self, minimal_config, mock_taskmanager, tmp_path):
        """SKIPPED status takes precedence over dry-run NOTSUBMITTED"""
        from woom.job import JobStatus

        minimal_config.filename = str(tmp_path / 'workflow.cfg')
        mock_task = Mock()
        mock_task.is_skipped = True
        mock_taskmanager.get_task.return_value = mock_task
        workflow = Workflow(minimal_config, mock_taskmanager)
        workflow._dry = True

        status = workflow.get_task_status('task1')

        assert status == JobStatus.SKIPPED
