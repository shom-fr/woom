#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for workflow.py module
"""
import os
from unittest.mock import Mock, mock_open, patch

import configobj
import pytest

from woom import hosts as whosts
from woom import iters as witers
from woom import job as wjob
from woom import tasks as wtasks
from woom import workflow as wworkflow


@pytest.fixture
def workflow_config(tmp_path):
    """Create a minimal workflow configuration"""
    config = configobj.ConfigObj()
    config["app"] = {"name": "test_app", "conf": "test_conf", "exp": "test_exp"}
    config["cycles"] = {
        "begin_date": None,
        "end_date": None,
        "freq": None,
        "ncycles": 0,
        "round": None,
        "indep": False,
        "as_intervals": True,
    }
    config["ensemble"] = {"size": None, "skip": None, "label": "member", "tasks": None, "iters": {}}
    config["params"] = {"hosts": {}, "tasks": {}}
    config["env_vars"] = {}
    config["groups"] = {}
    config["stages"] = {"prolog": {}, "cycles": {}, "epilog": {}, "dry_run": False, "update": False}
    config.filename = str(tmp_path / "workflow.cfg")
    return config


@pytest.fixture
def mock_taskmanager():
    """Create a mock TaskManager"""
    manager = Mock(spec=wtasks.TaskManager)
    host = Mock(spec=whosts.Host)
    host.name = "test_host"
    host.get_params.return_value = {"scratch_dir": "/scratch"}
    manager.host = host

    # Mock task
    task = Mock()
    task.name = "test_task"
    task.get_run_dir.return_value = "/run/dir"
    task.export_commandline.return_value = "echo test"
    task.render_artifacts.return_value = {}
    task.export_scheduler_options.return_value = {}
    task.env = Mock()
    task.env.prepend_paths = Mock()
    task.env.vars_set = {}
    task.export.return_value = {
        "script_content": "#!/bin/bash\necho test",
        "scheduler_options": {},
        "artifacts": {},
    }

    manager.get_task.return_value = task
    return manager


class TestWorkflowInit:
    """Test Workflow initialization"""

    def test_init_from_config(self, workflow_config, mock_taskmanager):
        """Test initialization with config object"""
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        assert workflow is not None
        assert workflow._tm == mock_taskmanager

    def test_init_from_file(self, tmp_path, mock_taskmanager):
        """Test initialization from config file"""
        cfg_file = tmp_path / "workflow.cfg"
        cfg_file.write_text(
            """
[app]
name = test_app
[stages]
    [[prolog]]
    [[cycles]]
    [[epilog]]
[cycles]
[ensemble]
[params]
[env_vars]
[groups]
"""
        )

        with patch('woom.workflow.wconf.load_cfg') as mock_load:
            config = configobj.ConfigObj()
            config["app"] = {"name": "test", "conf": None, "exp": None}
            config["cycles"] = {
                "begin_date": None,
                "end_date": None,
                "freq": None,
                "ncycles": 0,
                "round": None,
                "indep": False,
                "as_intervals": True,
            }
            config["ensemble"] = {
                "size": None,
                "skip": None,
                "label": "member",
                "tasks": None,
                "iters": {},
            }
            config["params"] = {"hosts": {}, "tasks": {}}
            config["env_vars"] = {}
            config["groups"] = {}
            config["stages"] = {"prolog": {}, "cycles": {}, "epilog": {}}
            config.filename = str(cfg_file)
            mock_load.return_value = config

            workflow = wworkflow.Workflow(str(cfg_file), mock_taskmanager)
            assert workflow._cfgfile == str(cfg_file)

    def test_workflow_dir_is_absolute(self, workflow_config, mock_taskmanager, tmp_path):
        """Test that workflow_dir is always absolute path"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        assert os.path.isabs(workflow.workflow_dir)
        assert workflow.workflow_dir == str(tmp_path)

    def test_app_path_creation(self, workflow_config, mock_taskmanager):
        """Test app path is correctly constructed"""
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        app_path = workflow.get_app_path()
        assert "test_app" in app_path
        assert "test_conf" in app_path
        assert "test_exp" in app_path

    def test_cycles_empty_by_default(self, workflow_config, mock_taskmanager):
        """Test cycles are empty when not configured"""
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        assert workflow.cycles == []

    def test_cycles_generation(self, workflow_config, mock_taskmanager):
        """Test cycles are generated when configured"""
        workflow_config["cycles"]["begin_date"] = "2025-01-01"
        workflow_config["cycles"]["end_date"] = "2025-01-05"
        workflow_config["cycles"]["freq"] = "1D"
        workflow_config["stages"]["cycles"] = {"step1": ["task1"]}

        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        assert len(workflow.cycles) > 0

    def test_members_empty_by_default(self, workflow_config, mock_taskmanager):
        """Test ensemble members are empty when not configured"""
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        assert workflow.members == []
        assert workflow.nmembers == 0

    def test_members_generation(self, workflow_config, mock_taskmanager):
        """Test ensemble members are generated when configured"""
        workflow_config["ensemble"]["size"] = 5
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        assert len(workflow.members) == 5
        assert workflow.nmembers == 5


class TestWorkflowProperties:
    """Test Workflow properties"""

    def test_config_property(self, workflow_config, mock_taskmanager):
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        assert workflow.config == workflow_config

    def test_taskmanager_property(self, workflow_config, mock_taskmanager):
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        assert workflow.taskmanager == mock_taskmanager

    def test_host_property(self, workflow_config, mock_taskmanager):
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        assert workflow.host == mock_taskmanager.host

    def test_jobmanager_property(self, workflow_config, mock_taskmanager):
        """Test jobmanager is cached property"""
        mock_jobmanager = Mock()
        mock_taskmanager.host.get_jobmanager.return_value = mock_jobmanager

        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        jm1 = workflow.jobmanager
        jm2 = workflow.jobmanager

        assert jm1 is jm2  # Same instance (cached)
        mock_taskmanager.host.get_jobmanager.assert_called_once()


class TestWorkflowPaths:
    """Test path-related methods"""

    def test_get_app_path_default_separator(self, workflow_config, mock_taskmanager):
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        app_path = workflow.get_app_path()
        assert app_path == os.path.sep.join(["test_app", "test_conf", "test_exp"])

    def test_get_app_path_custom_separator(self, workflow_config, mock_taskmanager):
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        app_path = workflow.get_app_path(sep="-")
        assert app_path == "test_app-test_conf-test_exp"

    def test_get_task_path_simple(self, workflow_config, mock_taskmanager):
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        task_path = workflow.get_task_path("task1")
        assert "task1" in task_path
        assert "test_app" in task_path

    def test_get_task_path_with_cycle(self, workflow_config, mock_taskmanager):
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        cycle = witers.Cycle("2025-01-15")
        task_path = workflow.get_task_path("task1", cycle=cycle)
        assert "task1" in task_path
        assert "2025-01-15" in task_path

    def test_get_task_path_with_member(self, workflow_config, mock_taskmanager):
        workflow_config["ensemble"]["size"] = 3
        workflow_config["ensemble"]["tasks"] = ["task1"]
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        member = workflow.members[0]
        task_path = workflow.get_task_path("task1", member=member)
        assert "task1" in task_path
        assert member.label in task_path

    def test_get_submission_dir_safe_path(self, workflow_config, mock_taskmanager, tmp_path):
        """Test submission dir is always within workflow dir"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        subm_dir = workflow.get_submission_dir("task1", create=False)
        # Check that submission dir is relative to workflow dir, not absolute system path
        assert subm_dir.startswith(str(tmp_path))
        assert "jobs" in subm_dir

    def test_get_run_dir(self, workflow_config, mock_taskmanager, tmp_path):
        """Test get_run_dir renders the task run directory"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        run_dir = workflow.get_run_dir("task1")
        assert run_dir == "/run/dir"


class TestWorkflowTaskInputs:
    """Test task input generation"""

    def test_get_task_inputs_basic(self, workflow_config, mock_taskmanager, tmp_path):
        """Test basic task inputs generation"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        params, env_vars = workflow.get_task_inputs("task1")

        assert "task_name" in params
        assert params["task_name"] == "task1"
        assert "workflow" in params
        assert "task" in params
        assert "WOOM_TASK_NAME" in env_vars

    def test_get_task_inputs_with_cycle(self, workflow_config, mock_taskmanager, tmp_path):
        """Test task inputs with cycle"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        cycle = witers.Cycle("2025-01-15")

        params, env_vars = workflow.get_task_inputs("task1", cycle=cycle)

        assert "cycle" in params
        assert params["cycle"] == cycle
        assert "WOOM_CYCLE_BEGIN_DATE" in env_vars

    def test_get_task_inputs_with_member(self, workflow_config, mock_taskmanager, tmp_path):
        """Test task inputs with ensemble member"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow_config["ensemble"]["size"] = 3
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        member = workflow.members[0]

        params, env_vars = workflow.get_task_inputs("task1", member=member)

        assert "member" in params
        assert params["member"] == member

    def test_get_task_inputs_paths_safe(self, workflow_config, mock_taskmanager, tmp_path):
        """Test that all paths in task inputs are safe (not root)"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        params, env_vars = workflow.get_task_inputs("task1")

        # Check critical paths
        assert params["workflow_dir"] == str(tmp_path)
        assert params["submission_dir"].startswith(str(tmp_path))
        assert params["log_dir"].startswith(str(tmp_path))
        assert params["script_path"].startswith(str(tmp_path))


class TestWorkflowTaskMembers:
    """Test task member management"""

    def test_get_task_members_no_ensemble(self, workflow_config, mock_taskmanager):
        """Test get_task_members returns None when no ensemble"""
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        members = workflow.get_task_members("task1")
        assert members is None

    def test_get_task_members_with_ensemble(self, workflow_config, mock_taskmanager):
        """Test get_task_members returns members for ensemble tasks"""
        workflow_config["ensemble"]["size"] = 3
        workflow_config["ensemble"]["tasks"] = ["task1"]
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        members = workflow.get_task_members("task1")
        assert len(members) == 3

    def test_get_task_members_not_in_ensemble(self, workflow_config, mock_taskmanager):
        """Test get_task_members returns None for non-ensemble tasks"""
        workflow_config["ensemble"]["size"] = 3
        workflow_config["ensemble"]["tasks"] = ["task2"]
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        members = workflow.get_task_members("task1")
        assert members is None


class TestWorkflowSubmission:
    """Test task submission"""

    def test_get_submission_args(self, workflow_config, mock_taskmanager, tmp_path):
        """Test submission arguments generation"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        args = workflow._get_submission_args_("task1", None, None, [])

        assert "script" in args
        assert "content" in args
        assert "opts" in args
        assert "depend" in args
        assert "artifacts" in args
        # Ensure script path is safe
        assert args["script"].startswith(str(tmp_path))

    @patch('builtins.open', new_callable=mock_open)
    def test_submit_task(self, mock_file, workflow_config, mock_taskmanager, tmp_path):
        """Test task submission creates script and submits job"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        mock_job = Mock()
        mock_job.jobid = "12345"
        workflow.jobmanager.submit = Mock(return_value=mock_job)

        with patch('os.path.exists', return_value=True):
            job = workflow.submit_task("task1")

        assert job is not None
        assert mock_file.called
        workflow.jobmanager.submit.assert_called_once()

    def test_submit_task_fake(self, workflow_config, mock_taskmanager, tmp_path, capsys):
        """Test fake task submission (dry run)"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        workflow.jobmanager.get_submission_command = Mock(return_value="submission_command")

        jobid = workflow.submit_task_fake("task1")

        assert jobid is not None
        assert isinstance(jobid, str)


class TestWorkflowStatus:
    """Test workflow status operations"""

    def test_get_task_status_not_submitted(self, workflow_config, mock_taskmanager, tmp_path):
        """Test status when task not submitted"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        with patch('os.path.exists', return_value=False):
            status = workflow.get_task_status("task1")

        assert status == wjob.JobStatus.NOTSUBMITTED

    def test_get_task_status_success(self, workflow_config, mock_taskmanager, tmp_path):
        """Test status when task succeeded"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        submission_dir = workflow.get_submission_dir("task1")

        mock_job = Mock()
        mock_job.jobid = "123"

        def mock_exists(path):
            return path == submission_dir or "job.json" in path or "job.status" in path

        with (
            patch('os.path.exists', side_effect=mock_exists),
            patch('builtins.open', mock_open(read_data="0")),
            patch.object(workflow.jobmanager, 'load_job', return_value=mock_job),
        ):
            status = workflow.get_task_status("task1")

        assert status == wjob.JobStatus.SUCCESS

    def test_get_status_dataframe(self, workflow_config, mock_taskmanager, tmp_path):
        """Test get_status returns DataFrame"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow_config["stages"]["prolog"] = {"step1": ["task1"]}
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        with patch('os.path.exists', return_value=False):
            df = workflow.get_status()

        assert len(df) > 0
        assert "STATUS" in df.columns
        assert "TASK" in df.columns


class TestWorkflowCleaning:
    """Test workflow cleaning operations"""

    def test_clean_task(self, workflow_config, mock_taskmanager, tmp_path):
        """Test cleaning task files"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        # Create fake submission dir with files
        subm_dir = tmp_path / "jobs" / workflow.get_app_path() / "task1"
        subm_dir.mkdir(parents=True)
        (subm_dir / "job.sh").write_text("#!/bin/bash")
        (subm_dir / "job.out").write_text("output")

        workflow.clean_task("task1")

        # Files should be removed
        assert not (subm_dir / "job.sh").exists()
        assert not (subm_dir / "job.out").exists()

    def test_clean_dry_run(self, workflow_config, mock_taskmanager, tmp_path):
        """Test clean in dry run mode doesn't delete files"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        workflow._dry = True

        subm_dir = tmp_path / "jobs" / "task1"
        subm_dir.mkdir(parents=True)
        test_file = subm_dir / "job.sh"
        test_file.write_text("#!/bin/bash")

        workflow.clean_task("task1")

        # File should still exist in dry run
        assert test_file.exists()


class TestWorkflowIteration:
    """Test workflow iteration"""

    def test_iter_basic(self, workflow_config, mock_taskmanager, tmp_path):
        """Test iterating over workflow tasks"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow_config["stages"]["prolog"] = {"step1": ["task1", "task2"]}
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        tasks = list(workflow)

        assert len(tasks) == 2
        assert tasks[0][0] == "task1"
        assert tasks[1][0] == "task2"

    def test_iter_with_cycles(self, workflow_config, mock_taskmanager, tmp_path):
        """Test iterating with cycles"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow_config["cycles"]["begin_date"] = "2025-01-01"
        workflow_config["cycles"]["end_date"] = "2025-01-03"
        workflow_config["cycles"]["freq"] = "1D"
        workflow_config["stages"]["cycles"] = {"step": ["task1"]}

        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)
        tasks = list(workflow)

        # Should have multiple tasks (one per cycle)
        assert len(tasks) > 1


class TestWorkflowDisplay:
    """Test workflow display methods"""

    def test_show_overview(self, workflow_config, mock_taskmanager, tmp_path, capsys):
        """Test show_overview displays workflow info"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        workflow.show_overview()

        captured = capsys.readouterr()
        assert "APP" in captured.out
        assert "TASK TREE" in captured.out
        assert "CYCLES" in captured.out
        assert "ENSEMBLE" in captured.out

    def test_show_status(self, workflow_config, mock_taskmanager, tmp_path, capsys):
        """Test show_status displays task status"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow_config["stages"]["prolog"] = {"step1": ["task1"]}
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        with patch('os.path.exists', return_value=False):
            workflow.show_status()

        captured = capsys.readouterr()
        assert len(captured.out) > 0


class TestWorkflowArtifacts:
    """Test artifact management"""

    def test_get_task_artifacts(self, workflow_config, mock_taskmanager, tmp_path):
        """Test getting task artifacts"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        mock_taskmanager.get_task.return_value.render_artifacts.return_value = {
            "output": "/path/to/output.nc"
        }

        artifacts = workflow.get_task_artifacts("task1")
        assert "output" in artifacts

    def test_get_artifact(self, workflow_config, mock_taskmanager, tmp_path):
        """Test getting specific artifact"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        mock_taskmanager.get_task.return_value.render_artifacts.return_value = {
            "output": "/path/to/output.nc"
        }

        artifact_path = workflow.get_artifact("output", "task1")
        assert artifact_path == "/path/to/output.nc"


class TestWorkflowSafety:
    """Test that workflow operations are safe and don't touch system root"""

    def test_no_absolute_paths_at_root(self, workflow_config, mock_taskmanager, tmp_path):
        """Ensure workflow never creates paths at system root"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        # Test various path methods
        subm_dir = workflow.get_submission_dir("task1", create=False)
        assert not subm_dir.startswith("/jobs")  # Not at root
        assert subm_dir.startswith(str(tmp_path))  # Within temp dir

        params, _ = workflow.get_task_inputs("task1")
        assert params["workflow_dir"] == str(tmp_path)
        assert not params["submission_dir"].startswith("/jobs")

    def test_workflow_dir_always_safe(self, workflow_config, mock_taskmanager, tmp_path):
        """Test workflow_dir is always within a safe location"""
        workflow_config.filename = str(tmp_path / "workflow.cfg")
        workflow = wworkflow.Workflow(workflow_config, mock_taskmanager)

        # Workflow dir should be the parent of the config file
        assert workflow.workflow_dir == str(tmp_path)
        assert os.path.isabs(workflow.workflow_dir)

        # Should not be root
        assert workflow.workflow_dir != "/"
