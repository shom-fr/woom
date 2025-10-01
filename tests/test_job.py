#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for job.py module
"""
import json
from unittest.mock import MagicMock, Mock, patch

import pytest

from woom import job as wjob


class TestJobStatus:
    """Test JobStatus enum"""

    def test_status_values(self):
        assert wjob.JobStatus.FAILED.value == -6
        assert wjob.JobStatus.SUCCESS.value == -4
        assert wjob.JobStatus.RUNNING.value == 2

    def test_is_running(self):
        assert wjob.JobStatus.RUNNING.is_running()
        assert wjob.JobStatus.PENDING.is_running()
        assert not wjob.JobStatus.SUCCESS.is_running()

    def test_is_not_running(self):
        assert wjob.JobStatus.SUCCESS.is_not_running()
        assert wjob.JobStatus.FAILED.is_not_running()
        assert not wjob.JobStatus.RUNNING.is_not_running()

    def test_is_unknown(self):
        assert wjob.JobStatus.UNKNOWN.is_unknown()
        assert not wjob.JobStatus.RUNNING.is_unknown()

    def test_is_killed(self):
        assert wjob.JobStatus.KILLED.is_killed()
        assert not wjob.JobStatus.FINISHED.is_killed()

    def test_jobid_property(self):
        status = wjob.JobStatus.RUNNING
        status.jobid = "12345"
        assert status.jobid == "12345"


class TestJob:
    """Test Job class"""

    def setup_method(self):
        """Setup for each test"""
        self.mock_manager = Mock()
        self.mock_manager.__class__.__name__ = "BackgroundJobManager"

    def test_init(self):
        job = wjob.Job(
            manager=self.mock_manager,
            name="test_job",
            script="/path/to/script.sh",
            args=["bash", "/path/to/script.sh"],
            jobid="12345",
        )
        assert job.name == "test_job"
        assert job.jobid == "12345"
        assert job.script == "/path/to/script.sh"

    def test_init_with_string_status(self):
        job = wjob.Job(manager=self.mock_manager, name="test", script="/script.sh", args=[], status="RUNNING")
        assert job.status == wjob.JobStatus.RUNNING

    def test_str_representation(self):
        job = wjob.Job(manager=self.mock_manager, name="test", script="/script.sh", args=[], jobid="12345")
        assert str(job) == "12345"

    def test_repr(self):
        job = wjob.Job(
            manager=self.mock_manager,
            name="test_job",
            script="/script.sh",
            args=[],
            jobid="12345",
            status="RUNNING",
        )
        repr_str = repr(job)
        assert "test_job" in repr_str
        assert "12345" in repr_str

    def test_to_dict(self):
        job = wjob.Job(
            manager=self.mock_manager,
            name="test",
            script="/script.sh",
            args=["bash"],
            jobid="12345",
        )
        job_dict = job.to_dict()
        assert job_dict["name"] == "test"
        assert job_dict["jobid"] == "12345"
        assert job_dict["manager"] == "BackgroundJobManager"

    def test_dump(self, tmp_path):
        job = wjob.Job(
            manager=self.mock_manager,
            name="test",
            script=str(tmp_path / "job.sh"),
            args=["bash"],
            jobid="12345",
        )
        json_path = job.dump()
        assert json_path.endswith(".json")

        with open(json_path) as f:
            data = json.load(f)
        assert data["jobid"] == "12345"

    def test_load(self, tmp_path):
        json_file = tmp_path / "job.json"
        job_data = {
            "manager": "BackgroundJobManager",
            "name": "test",
            "script": "/script.sh",
            "args": ["bash"],
            "jobid": "12345",
            "queue": None,
            "status": "RUNNING",
            "submission_date": "2025-01-01",
        }
        with open(json_file, 'w') as f:
            json.dump(job_data, f)

        job = wjob.Job.load(self.mock_manager, str(json_file), append=False)
        assert job.jobid == "12345"
        assert job.name == "test"


class TestBackgroundJobManager:
    """Test BackgroundJobManager class"""

    def test_init(self):
        manager = wjob.BackgroundJobManager()
        assert manager.jobs == []

    def test_from_scheduler(self):
        manager = wjob.BackgroundJobManager.from_scheduler("background")
        assert isinstance(manager, wjob.BackgroundJobManager)

    def test_from_scheduler_slurm(self):
        manager = wjob.BackgroundJobManager.from_scheduler("slurm")
        assert isinstance(manager, wjob.SlurmJobManager)

    def test_from_scheduler_pbspro(self):
        manager = wjob.BackgroundJobManager.from_scheduler("pbspro")
        assert isinstance(manager, wjob.PbsproJobManager)

    def test_from_scheduler_invalid(self):
        with pytest.raises(AssertionError):
            wjob.BackgroundJobManager.from_scheduler("invalid")

    def test_get_command_args(self):
        args = wjob.BackgroundJobManager.get_command_args("submit", script="/path/to/script.sh")
        assert "bash" in args
        assert "/path/to/script.sh" in args

    def test_get_job(self):
        manager = wjob.BackgroundJobManager()
        mock_job = Mock()
        mock_job.jobid = "12345"
        manager.jobs.append(mock_job)

        job = manager.get_job("12345")
        assert job == mock_job

    def test_get_job_not_found(self):
        manager = wjob.BackgroundJobManager()
        job = manager.get_job("99999")
        assert job is None

    def test_contains(self):
        manager = wjob.BackgroundJobManager()
        mock_job = Mock()
        mock_job.jobid = "12345"
        manager.jobs.append(mock_job)

        assert "12345" in manager
        assert "99999" not in manager

    def test_get_jobs_all(self):
        manager = wjob.BackgroundJobManager()
        mock_job1 = Mock()
        mock_job1.id = "1"
        mock_job2 = Mock()
        mock_job2.id = "2"
        manager.jobs = [mock_job1, mock_job2]

        jobs = manager.get_jobs()
        assert len(jobs) == 2

    def test_get_jobs_by_id(self):
        manager = wjob.BackgroundJobManager()
        mock_job1 = Mock()
        mock_job1.id = "1"
        mock_job2 = Mock()
        mock_job2.id = "2"
        manager.jobs = [mock_job1, mock_job2]

        jobs = manager.get_jobs(jobids="1")
        assert len(jobs) == 1
        assert jobs[0].id == "1"

    @patch('subprocess.Popen')
    def test_submit(self, mock_popen, tmp_path):
        mock_process = MagicMock()
        mock_process.pid = 12345
        mock_process.args = ["bash", "script.sh"]
        mock_popen.return_value = mock_process

        manager = wjob.BackgroundJobManager()
        script = tmp_path / "script.sh"
        script.write_text("#!/bin/bash\necho test")

        job = manager.submit(script=str(script), opts={"name": "test_job"})

        assert job is not None
        assert job.jobid == "12345"
        assert len(manager.jobs) == 1


class TestScheduledJob:
    """Test ScheduledJob class"""

    def test_init(self):
        mock_manager = Mock()
        job = wjob.ScheduledJob(
            manager=mock_manager, name="test", script="/script.sh", args=[], jobid="12345"
        )
        assert job.jobid == "12345"


class TestSlurmJobManager:
    """Test SlurmJobManager class"""

    def test_init(self):
        manager = wjob.SlurmJobManager()
        assert manager is not None

    def test_commands_structure(self):
        assert "submit" in wjob.SlurmJobManager.commands
        assert "status" in wjob.SlurmJobManager.commands
        assert "delete" in wjob.SlurmJobManager.commands

    def test_status_names(self):
        assert wjob.JobStatus.RUNNING in wjob.SlurmJobManager.status_names.values()
        assert wjob.JobStatus.PENDING in wjob.SlurmJobManager.status_names.values()


class TestPbsproJobManager:
    """Test PbsproJobManager class"""

    def test_init(self):
        manager = wjob.PbsproJobManager()
        assert manager is not None

    def test_commands_structure(self):
        assert "submit" in wjob.PbsproJobManager.commands
        assert "status" in wjob.PbsproJobManager.commands
        assert "delete" in wjob.PbsproJobManager.commands

    def test_status_names(self):
        assert wjob.JobStatus.RUNNING in wjob.PbsproJobManager.status_names.values()
        assert wjob.JobStatus.INQUEUE in wjob.PbsproJobManager.status_names.values()
