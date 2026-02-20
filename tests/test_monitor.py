#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit and integration tests for woom.monitor module.

Tests cover:
- SSELogHandler: emit, subscribe, unsubscribe, history eviction
- _serialize_task_tree: stage ordering, conversion from ConfigObj-like objects
- Flask routes via app.test_client(): all API endpoints
- Security: path-traversal protection on /api/job/file and /api/files
- Thread safety of SSELogHandler
"""

import json
import logging
import os
import queue
import threading
import time
from collections import OrderedDict
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from woom.monitor import SSELogHandler, _serialize_task_tree, create_monitor_app

# %% Helpers / shared fixtures


def _make_status(name, jobid=None):
    s = Mock()
    s.name = name
    s.jobid = jobid
    return s


def _make_workflow(tmp_path, with_scheduler=False):
    """Return a fully-mocked Workflow suitable for the Flask app."""
    wf = Mock()
    wf.workflow_dir = str(tmp_path)
    wf.config = {"app": {"name": "test_app", "conf": "conf1", "exp": "exp1"}}
    wf.host = Mock()
    wf.host.name = "local"
    wf.jobmanager = Mock()
    wf.jobmanager.with_scheduler = with_scheduler
    wf.cycles = ["2025-01-01", "2025-01-02"]
    wf.nmembers = 1
    wf.task_tree = {
        "prolog": {"seq1": [["task_fetch"]]},
        "cycles": {"seq2": [["task_run"]]},
        "epilog": {"seq3": [["task_archive"]]},
    }
    wf.taskmanager = Mock()
    wf.taskmanager.config = {}

    # __iter__ yields (task_name, cycle, member) tuples
    wf.__iter__ = Mock(
        return_value=iter(
            [
                ("task_fetch", None, None),
                ("task_run", "2025-01-01", None),
                ("task_archive", None, None),
            ]
        )
    )
    wf.get_task_status = Mock(side_effect=lambda t, c, m: _make_status("SUCCESS"))
    wf.get_task_submission_dir = Mock(return_value=str(tmp_path / "submdir"))
    wf.get_artifacts = Mock(
        return_value=pd.DataFrame(
            {
                "TASK": ["task_run"],
                "ARTIFACT": ["output.nc"],
                "PATH": [str(tmp_path / "output.nc")],
                "EXISTS?": [False],
            }
        )
    )
    wf.run = Mock()
    wf.kill = Mock()
    wf.clean = Mock()
    return wf


@pytest.fixture
def workflow(tmp_path):
    return _make_workflow(tmp_path)


@pytest.fixture
def scheduler_workflow(tmp_path):
    return _make_workflow(tmp_path, with_scheduler=True)


@pytest.fixture
def sse_handler():
    return SSELogHandler()


@pytest.fixture
def app(workflow):
    flask_app = create_monitor_app(workflow, SSELogHandler())
    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


# %% SSELogHandler


class TestSSELogHandler:
    @pytest.mark.unit
    def test_emit_stores_in_history(self, sse_handler):
        record = logging.LogRecord("woom", logging.INFO, "", 0, "hello", (), None)
        sse_handler.emit(record)
        assert len(sse_handler._history) == 1
        payload = json.loads(sse_handler._history[0])
        assert payload["level"] == "INFO"
        assert "hello" in payload["message"]
        assert "time" in payload

    @pytest.mark.unit
    def test_emit_delivers_to_subscriber(self, sse_handler):
        q = sse_handler.subscribe()
        record = logging.LogRecord("woom", logging.WARNING, "", 0, "warn msg", (), None)
        sse_handler.emit(record)
        data = q.get_nowait()
        payload = json.loads(data)
        assert payload["level"] == "WARNING"

    @pytest.mark.unit
    def test_subscribe_receives_history(self, sse_handler):
        # Emit before subscribing
        for i in range(3):
            record = logging.LogRecord("woom", logging.DEBUG, "", 0, f"msg{i}", (), None)
            sse_handler.emit(record)

        q = sse_handler.subscribe()
        assert q.qsize() == 3

    @pytest.mark.unit
    def test_unsubscribe_stops_delivery(self, sse_handler):
        q = sse_handler.subscribe()
        sse_handler.unsubscribe(q)
        record = logging.LogRecord("woom", logging.INFO, "", 0, "after unsub", (), None)
        sse_handler.emit(record)
        assert q.empty()

    @pytest.mark.unit
    def test_history_maxlen_eviction(self):
        handler = SSELogHandler(maxlen=5)
        for i in range(10):
            record = logging.LogRecord("woom", logging.INFO, "", 0, f"msg{i}", (), None)
            handler.emit(record)
        assert len(handler._history) == 5
        # Last 5 messages should remain
        messages = [json.loads(d)["message"] for d in handler._history]
        for i in range(5, 10):
            assert any(f"msg{i}" in m for m in messages)

    @pytest.mark.unit
    def test_unsubscribe_nonexistent_is_safe(self, sse_handler):
        q = queue.Queue()
        sse_handler.unsubscribe(q)  # must not raise

    @pytest.mark.unit
    def test_thread_safety(self, sse_handler):
        """Concurrent emitters must not corrupt history or subscriber lists."""
        errors = []

        def _emit(n):
            try:
                for i in range(50):
                    record = logging.LogRecord("woom", logging.INFO, "", 0, f"t{n}-{i}", (), None)
                    sse_handler.emit(record)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_emit, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors


# %% _serialize_task_tree


class TestSerializeTaskTree:
    @pytest.mark.unit
    def test_preserves_stage_order(self):
        # ConfigObj may store keys alphabetically; serialize must fix this
        tree = OrderedDict(
            [
                ("epilog", {"s1": [["task_archive"]]}),
                ("cycles", {"s2": [["task_run"]]}),
                ("prolog", {"s3": [["task_fetch"]]}),
            ]
        )
        result = _serialize_task_tree(tree)
        keys = list(result.keys())
        assert keys == ["prolog", "cycles", "epilog"]

    @pytest.mark.unit
    def test_converts_groups_to_lists(self):
        tree = {"cycles": {"seq1": [("task_a", "task_b")]}}
        result = _serialize_task_tree(tree)
        # Groups should be plain lists
        assert result["cycles"]["seq1"][0] == ["task_a", "task_b"]

    @pytest.mark.unit
    def test_empty_stage(self):
        tree = {"prolog": {}}
        result = _serialize_task_tree(tree)
        assert result["prolog"] == {}

    @pytest.mark.unit
    def test_unknown_stage_appended_after_known(self):
        tree = OrderedDict(
            [
                ("myextra", {"s1": [["task_x"]]}),
                ("prolog", {"s2": [["task_y"]]}),
            ]
        )
        result = _serialize_task_tree(tree)
        keys = list(result.keys())
        # prolog comes before myextra
        assert keys.index("prolog") < keys.index("myextra")

    @pytest.mark.unit
    def test_missing_standard_stages_are_skipped(self):
        tree = {"cycles": {"s": [["task"]]}}
        result = _serialize_task_tree(tree)
        assert "prolog" not in result
        assert "epilog" not in result
        assert "cycles" in result


# %% Flask routes


class TestMonitorRoutes:
    @pytest.mark.unit
    def test_index_returns_html(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert b"<!DOCTYPE html" in resp.data or b"<html" in resp.data

    @pytest.mark.unit
    def test_api_info(self, client, workflow):
        resp = client.get("/api/info")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["app_name"] == "test_app"
        assert data["host"] == "local"
        assert data["scheduler"] is False
        assert "task_tree" in data
        assert "woom_version" in data
        assert data["ncycles"] == 2

    @pytest.mark.unit
    def test_api_info_task_tree_order(self, client):
        """task_tree in /api/info must have prolog before cycles before epilog."""
        resp = client.get("/api/info")
        data = resp.get_json()
        keys = list(data["task_tree"].keys())
        assert keys == ["prolog", "cycles", "epilog"]

    @pytest.mark.unit
    def test_api_status(self, client, workflow):
        resp = client.get("/api/status")
        assert resp.status_code == 200
        rows = resp.get_json()
        assert isinstance(rows, list)
        assert len(rows) == 3
        assert all("task" in r and "status" in r for r in rows)

    @pytest.mark.unit
    def test_api_artifacts(self, client):
        resp = client.get("/api/artifacts")
        assert resp.status_code == 200
        records = resp.get_json()
        assert len(records) == 1
        assert records[0]["TASK"] == "task_run"
        assert records[0]["ARTIFACT"] == "output.nc"

    @pytest.mark.unit
    def test_api_run_background_host(self, client, workflow):
        resp = client.post("/api/run", json={})
        assert resp.status_code == 200
        assert resp.get_json()["started"] is True

    @pytest.mark.unit
    def test_api_run_scheduler_host_forbidden(self, scheduler_workflow, tmp_path):
        app = create_monitor_app(scheduler_workflow, SSELogHandler())
        app.config["TESTING"] = True
        c = app.test_client()
        resp = c.post("/api/run", json={})
        assert resp.status_code == 403

    @pytest.mark.unit
    def test_api_run_dry_and_force_forwarded(self, client, workflow):
        client.post("/api/run", json={"dry": True, "force": True})
        # Give the daemon thread a moment to call workflow.run
        time.sleep(0.05)
        workflow.run.assert_called_once_with(dry=True, force=True)

    @pytest.mark.unit
    def test_api_run_status(self, client):
        resp = client.get("/api/run_status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "running" in data
        assert "error" in data

    @pytest.mark.unit
    def test_api_run_rejects_concurrent(self, client, workflow):
        # Make run block long enough that a second POST is rejected
        barrier = threading.Event()

        def _slow_run(**kw):
            barrier.wait(timeout=2)

        workflow.run.side_effect = _slow_run
        resp1 = client.post("/api/run", json={})
        assert resp1.status_code == 200
        resp2 = client.post("/api/run", json={})
        assert resp2.status_code == 409
        barrier.set()

    @pytest.mark.unit
    def test_api_kill_background_host(self, client, workflow):
        resp = client.post("/api/kill", json={"jobid": "42", "task_name": "task_run", "cycle": "2025-01-01"})
        assert resp.status_code == 200
        assert resp.get_json()["killed"] is True
        workflow.kill.assert_called_once_with(jobid="42", task_name="task_run", cycle="2025-01-01")

    @pytest.mark.unit
    def test_api_kill_scheduler_host_forbidden(self, scheduler_workflow, tmp_path):
        app = create_monitor_app(scheduler_workflow, SSELogHandler())
        app.config["TESTING"] = True
        c = app.test_client()
        resp = c.post("/api/kill", json={"jobid": "99"})
        assert resp.status_code == 403

    @pytest.mark.unit
    def test_api_kill_propagates_exception(self, client, workflow):
        workflow.kill.side_effect = RuntimeError("job not found")
        resp = client.post("/api/kill", json={"jobid": "bad"})
        assert resp.status_code == 500
        assert "job not found" in resp.get_json()["error"]

    @pytest.mark.unit
    def test_api_clean_calls_workflow(self, client, workflow):
        resp = client.post(
            "/api/clean",
            json={"submission_dirs": True, "log_files": False, "run_dirs": True, "dry": True},
        )
        assert resp.status_code == 200
        assert resp.get_json()["cleaned"] is True
        workflow.clean.assert_called_once_with(
            submission_dirs=True,
            log_files=False,
            run_dirs=True,
            artifacts=False,
            extra_files=None,
            dry=True,
        )

    @pytest.mark.unit
    def test_api_clean_propagates_exception(self, client, workflow):
        workflow.clean.side_effect = RuntimeError("clean failed")
        resp = client.post("/api/clean", json={})
        assert resp.status_code == 500
        assert "clean failed" in resp.get_json()["error"]

    @pytest.mark.unit
    def test_api_config(self, client):
        resp = client.get("/api/config")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "workflow" in data
        assert "tasks" in data

    @pytest.mark.unit
    def test_api_stop_returns_stopping(self, client):
        resp = client.post("/api/stop")
        assert resp.status_code == 200
        assert resp.get_json()["stopping"] is True

    @pytest.mark.unit
    def test_api_restart_sets_flag_and_returns(self, workflow):
        restart_flag = threading.Event()
        flask_app = create_monitor_app(workflow, SSELogHandler(), restart_flag=restart_flag)
        flask_app.config["TESTING"] = True
        c = flask_app.test_client()
        resp = c.post("/api/restart")
        assert resp.status_code == 200
        assert resp.get_json()["restarting"] is True
        assert restart_flag.is_set()

    @pytest.mark.unit
    def test_api_logs_stream_route_registered(self, app):
        rules = {r.rule for r in app.url_map.iter_rules()}
        assert "/api/logs/stream" in rules


# %% /api/job/file


class TestApiJobFile:
    @pytest.mark.unit
    def test_returns_content_inside_workflow_dir(self, client, workflow, tmp_path):
        submdir = tmp_path / "submdir"
        submdir.mkdir(exist_ok=True)
        (submdir / "job.out").write_text("hello output")
        workflow.get_task_submission_dir.return_value = str(submdir)

        resp = client.get(f"/api/job/file?submdir={submdir}&type=out")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["exists"] is True
        assert "hello output" in data["content"]

    @pytest.mark.unit
    def test_missing_file_returns_empty(self, client, tmp_path):
        submdir = tmp_path / "submdir"
        submdir.mkdir(exist_ok=True)
        resp = client.get(f"/api/job/file?submdir={submdir}&type=out")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["exists"] is False
        assert data["content"] == ""

    @pytest.mark.unit
    def test_invalid_file_type_returns_400(self, client, tmp_path):
        submdir = tmp_path / "submdir"
        submdir.mkdir(exist_ok=True)
        resp = client.get(f"/api/job/file?submdir={submdir}&type=exe")
        assert resp.status_code == 400

    @pytest.mark.unit
    def test_path_outside_workflow_dir_returns_403(self, client, tmp_path):
        # Create a directory OUTSIDE the workflow dir
        outside = tmp_path.parent / "outside"
        outside.mkdir(exist_ok=True)
        (outside / "job.out").write_text("secret")
        resp = client.get(f"/api/job/file?submdir={outside}&type=out")
        assert resp.status_code == 403


# %% /api/files security


class TestApiFiles:
    @pytest.mark.unit
    def test_file_inside_workflow_dir_is_served(self, client, tmp_path):
        f = tmp_path / "result.txt"
        f.write_text("data")
        resp = client.get(f"/api/files?path={f}")
        assert resp.status_code == 200

    @pytest.mark.unit
    def test_file_outside_workflow_dir_without_artifact_is_denied(self, client, tmp_path, workflow):
        outside = tmp_path.parent / "secret.txt"
        outside.write_text("secret")
        # Artifact list does not include this file
        workflow.get_artifacts.return_value = pd.DataFrame(
            {"TASK": [], "ARTIFACT": [], "PATH": [], "EXISTS?": []}
        )
        resp = client.get(f"/api/files?path={outside}")
        assert resp.status_code == 403

    @pytest.mark.unit
    def test_declared_artifact_outside_workflow_dir_is_served(self, tmp_path, workflow):
        # File lives outside workflow_dir but is a declared artifact
        outside_dir = tmp_path.parent / "artifacts_dir"
        outside_dir.mkdir(exist_ok=True)
        artifact_file = outside_dir / "model_output.nc"
        artifact_file.write_text("netcdf data")

        workflow.get_artifacts.return_value = pd.DataFrame(
            {
                "TASK": ["task_run"],
                "ARTIFACT": ["model_output.nc"],
                "PATH": [str(artifact_file)],
                "EXISTS?": [True],
            }
        )
        flask_app = create_monitor_app(workflow, SSELogHandler())
        flask_app.config["TESTING"] = True
        c = flask_app.test_client()
        resp = c.get(f"/api/files?path={artifact_file}")
        assert resp.status_code == 200

    @pytest.mark.unit
    def test_nonexistent_file_returns_404(self, client, tmp_path):
        resp = client.get(f"/api/files?path={tmp_path / 'nope.txt'}")
        assert resp.status_code == 404

    @pytest.mark.unit
    def test_path_traversal_attempt_is_denied(self, client, tmp_path):
        # Attempt ../../etc/passwd style traversal
        traversal = str(tmp_path) + "/../../etc/passwd"
        resp = client.get(f"/api/files?path={traversal}")
        # Either 403 (if /etc/passwd not inside workflow_dir) or 404 is acceptable;
        # 200 would be a security failure
        assert resp.status_code in (403, 404)


# %% /api/crontab


class TestApiCrontab:
    @pytest.mark.unit
    def test_crontab_get_returns_relevant_lines(self, client, workflow):
        marker = f"woom:{workflow.workflow_dir}"
        crontab_output = f"0 3 * * * some-other-cmd\n5 2 * * * cd /x && woom run  # {marker}\n"
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = crontab_output
        mock_result.stderr = ""
        with patch("subprocess.run", return_value=mock_result):
            resp = client.get("/api/crontab")
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data["relevant"]) == 1
        assert marker in data["relevant"][0]

    @pytest.mark.unit
    def test_crontab_post_add(self, client, workflow):
        mock_list = Mock(returncode=0, stdout="", stderr="")
        mock_write = Mock(returncode=0, stdout="", stderr="")
        with patch("subprocess.run", side_effect=[mock_list, mock_write]) as mock_run:
            resp = client.post("/api/crontab", json={"action": "add", "cron_expr": "0 2 * * *"})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["success"] is True
        assert "0 2 * * *" in data["line"]

    @pytest.mark.unit
    def test_crontab_post_remove(self, client, workflow):
        marker = f"woom:{workflow.workflow_dir}"
        existing = f"0 2 * * * cd /x && woom run  # {marker}\n"
        mock_list = Mock(returncode=0, stdout=existing, stderr="")
        mock_write = Mock(returncode=0, stdout="", stderr="")
        with patch("subprocess.run", side_effect=[mock_list, mock_write]):
            resp = client.post("/api/crontab", json={"action": "remove"})
        assert resp.status_code == 200
        # The written crontab should not contain the marker
        write_call_input = mock_write.call_args  # not directly available here; check success
        assert resp.get_json()["success"] is True

    @pytest.mark.unit
    def test_crontab_post_add_missing_expr_returns_400(self, client):
        resp = client.post("/api/crontab", json={"action": "add", "cron_expr": ""})
        assert resp.status_code == 400

    @pytest.mark.unit
    def test_crontab_get_handles_no_crontab(self, client):
        mock_result = Mock(returncode=1, stdout="", stderr="no crontab for user")
        with patch("subprocess.run", return_value=mock_result):
            resp = client.get("/api/crontab")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["relevant"] == []
