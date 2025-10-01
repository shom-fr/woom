#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pytest configuration and shared fixtures for woom tests
"""
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest


# Add the parent directory to path to import woom
@pytest.fixture(scope="session", autouse=True)
def add_woom_to_path():
    """Add woom package to Python path"""
    woom_root = Path(__file__).parent.parent
    if str(woom_root) not in sys.path:
        sys.path.insert(0, str(woom_root))


@pytest.fixture
def temp_workflow_dir(tmp_path):
    """Create a temporary workflow directory structure"""
    workflow_dir = tmp_path / "workflow"
    workflow_dir.mkdir()

    # Create subdirectories
    (workflow_dir / "bin").mkdir()
    (workflow_dir / "lib").mkdir()
    (workflow_dir / "log").mkdir()
    (workflow_dir / "jobs").mkdir()

    return workflow_dir


@pytest.fixture
def sample_workflow_config(temp_workflow_dir):
    """Create a sample workflow configuration file"""
    cfg_file = temp_workflow_dir / "workflow.cfg"
    cfg_content = """
[app]
name = test_app
conf = test_conf
exp = test_exp

[cycles]
begin_date = 2025-01-01
end_date = 2025-01-05
freq = 1D

[params]

[stages]
    [[prolog]]
    fetch = task_fetch

    [[cycles]]
    run = task_run

    [[epilog]]
    archive = task_archive
"""
    cfg_file.write_text(cfg_content)
    return cfg_file


@pytest.fixture
def sample_tasks_config(temp_workflow_dir):
    """Create a sample tasks configuration file"""
    cfg_file = temp_workflow_dir / "tasks.cfg"
    cfg_content = """
[task_fetch]
    [[content]]
    commandline = echo "Fetching data"
    run_dir = {{ scratch_dir }}/{{ task_name }}

[task_run]
    [[content]]
    commandline = echo "Running simulation"
    run_dir = {{ scratch_dir }}/{{ task_name }}

[task_archive]
    [[content]]
    commandline = echo "Archiving results"
    run_dir = {{ scratch_dir }}/{{ task_name }}
"""
    cfg_file.write_text(cfg_content)
    return cfg_file


@pytest.fixture
def sample_hosts_config(temp_workflow_dir):
    """Create a sample hosts configuration file"""
    cfg_file = temp_workflow_dir / "hosts.cfg"
    cfg_content = """
[local]
patterns = *
scheduler = background

[test_cluster]
patterns = test-node*
scheduler = slurm
    [[queues]]
    seq = normal
    omp = parallel
"""
    cfg_file.write_text(cfg_content)
    return cfg_file


@pytest.fixture
def mock_host():
    """Create a mock Host object"""
    host = Mock()
    host.name = "test_host"
    host.config = {
        "scheduler": "background",
        "queues": {"seq": "normal"},
        "dirs": {"scratch": "/scratch", "work": "/work"},
        "params": {},
    }
    host.__getitem__ = lambda self, key: host.config[key]
    host.get_params = Mock(return_value={"scratch_dir": "/scratch"})
    host.get_queue = Mock(side_effect=lambda q: host.config["queues"].get(q, q))

    from woom import env as wenv

    host.get_env = Mock(return_value=wenv.EnvConfig())

    return host


@pytest.fixture
def mock_task():
    """Create a mock Task object"""
    task = Mock()
    task.name = "test_task"
    task.config = {
        "content": {"commandline": "echo test", "run_dir": "/run/dir", "env": None},
        "artifacts": {},
        "submit": {"queue": None, "memory": None, "time": None, "mail": None, "extra": {}},
    }
    task.get_run_dir = Mock(return_value="/run/dir")
    task.export_commandline = Mock(return_value="echo test")
    task.export_artifacts_checking = Mock(return_value="")
    task.render_artifacts = Mock(return_value={})
    task.export_scheduler_options = Mock(return_value={})

    return task


@pytest.fixture
def mock_taskmanager(mock_host, mock_task):
    """Create a mock TaskManager object"""
    manager = Mock()
    manager.host = mock_host
    manager.get_task = Mock(return_value=mock_task)

    return manager


@pytest.fixture
def mock_workflow(mock_taskmanager, temp_workflow_dir):
    """Create a mock Workflow object"""
    workflow = Mock()
    workflow.taskmanager = mock_taskmanager
    workflow.host = mock_taskmanager.host
    workflow.workflow_dir = str(temp_workflow_dir)
    workflow.cycles = []
    workflow.members = []
    workflow.nmembers = 0
    workflow.get_task = mock_taskmanager.get_task
    workflow.get_task_members = Mock(return_value=None)

    return workflow


@pytest.fixture
def sample_cycle():
    """Create a sample Cycle object"""
    from woom import iters as witers

    return witers.Cycle("2025-01-15", "2025-01-16")


@pytest.fixture
def sample_member():
    """Create a sample Member object"""
    from woom import iters as witers

    return witers.Member(1, 10)


@pytest.fixture
def mock_job():
    """Create a mock Job object"""
    job = Mock()
    job.jobid = "12345"
    job.name = "test_job"
    job.script = "/path/to/script.sh"
    job.status = Mock()
    job.status.name = "RUNNING"
    job.status.is_running = Mock(return_value=True)
    job.is_running = Mock(return_value=True)

    return job


@pytest.fixture
def mock_jobmanager(mock_job):
    """Create a mock JobManager object"""
    manager = Mock()
    manager.jobs = [mock_job]
    manager.get_job = Mock(return_value=mock_job)
    manager.submit = Mock(return_value=mock_job)
    manager.load_job = Mock(return_value=mock_job)

    return manager


@pytest.fixture(autouse=True)
def cleanup_configobj_cache():
    """Clean up configobj cache between tests"""
    yield
    # Reset any global caches if needed


@pytest.fixture
def reset_logging():
    """Reset logging configuration between tests"""
    import logging

    yield
    # Clear all loggers
    for logger in logging.Logger.manager.loggerDict.values():
        if isinstance(logger, logging.Logger):
            logger.handlers.clear()
            logger.filters.clear()


@pytest.fixture
def env_vars(monkeypatch):
    """Fixture to easily set environment variables"""

    def _set_env(**kwargs):
        for key, value in kwargs.items():
            monkeypatch.setenv(key, str(value))

    return _set_env


@pytest.fixture
def mock_subprocess():
    """Mock subprocess for testing job submissions"""
    from unittest.mock import MagicMock, patch

    with patch('subprocess.Popen') as mock_popen, patch('subprocess.run') as mock_run:
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.returncode = 0
        mock_process.stdout = b"output"
        mock_process.stderr = b""

        mock_popen.return_value = mock_process
        mock_run.return_value = mock_process

        yield {'popen': mock_popen, 'run': mock_run, 'process': mock_process}


# Markers for categorizing tests
def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "requires_scheduler: mark test as requiring a real scheduler")


# Skip tests that require real scheduler if not available
@pytest.fixture
def skip_if_no_scheduler():
    """Skip test if no real scheduler is available"""
    pytest.skip("Test requires a real job scheduler (slurm/pbspro)")
