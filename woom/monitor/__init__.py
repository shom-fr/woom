#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web monitor for woom workflows.

Provides a lightweight Flask-based web UI for launching, monitoring,
and stopping woom workflows, and browsing artifacts.

HTML/CSS/JS live in the sub-directories next to this package:
  woom/monitor/templates/index.html
  woom/monitor/static/monitor.css
  woom/monitor/static/monitor.js
"""

import json
import logging
import os
import queue
import shutil
import signal
import subprocess
import threading
import time
from collections import deque

# %% SSE log handler


class SSELogHandler(logging.Handler):
    """Logging handler that fans out log records to SSE subscribers.

    Keeps a rolling history (``deque(maxlen=500)``) so late-connecting
    clients receive recent events without missing anything.
    """

    def __init__(self, maxlen=500):
        super().__init__()
        self.setFormatter(logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s"))
        self._lock = threading.Lock()
        self._history = deque(maxlen=maxlen)
        self._subscribers = []

    def emit(self, record):
        try:
            msg = self.format(record)
            data = json.dumps({"level": record.levelname, "message": msg, "time": record.created})
            with self._lock:
                self._history.append(data)
                dead = []
                for q in self._subscribers:
                    try:
                        q.put_nowait(data)
                    except queue.Full:
                        dead.append(q)
                for q in dead:
                    self._subscribers.remove(q)
        except Exception:
            self.handleError(record)

    def subscribe(self):
        """Return a :class:`queue.Queue` pre-filled with history."""
        q = queue.Queue(maxsize=2000)
        with self._lock:
            for item in self._history:
                try:
                    q.put_nowait(item)
                except queue.Full:
                    break
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q):
        with self._lock:
            try:
                self._subscribers.remove(q)
            except ValueError:
                pass


# %% Helpers


def _serialize_task_tree(task_tree):
    """Convert the task_tree dict to a JSON-serializable structure.

    The task_tree from ``workflow.task_tree`` may contain ConfigObj objects;
    this converts them to plain Python lists/dicts.
    """
    result = {}
    for stage, sequences in task_tree.items():
        if not sequences:
            result[stage] = {}
            continue
        result[stage] = {}
        for seq_name, groups in sequences.items():
            result[stage][seq_name] = [list(g) for g in groups]
    return result


# %% Flask application factory


def create_monitor_app(workflow, sse_handler):
    """Create and return the Flask application.

    Templates are loaded from ``woom/monitor/templates/``;
    static assets from ``woom/monitor/static/`` (served at ``/static/``).

    Parameters
    ----------
    workflow : woom.workflow.Workflow
        Fully initialised workflow instance.
    sse_handler : SSELogHandler
        SSE log handler (already attached to ``logging.getLogger("woom")``).

    Returns
    -------
    flask.Flask
    """
    try:
        from flask import Flask, Response, abort, jsonify, render_template, request, send_file
    except ImportError as exc:
        raise ImportError(
            "Flask is required for the monitor subcommand. "
            "Install it with: pip install flask  (or: pip install woom[monitor])"
        ) from exc

    # Flask resolves template_folder / static_folder relative to __file__
    # (__file__ == woom/monitor/__init__.py → package root is woom/monitor/)
    app = Flask(__name__)

    # Mutable run-state dict protected by a lock
    _run_lock = threading.Lock()
    _run_state = {"running": False, "error": None}

    # %% HTML

    @app.route("/")
    def index():
        return render_template("index.html")

    # %% Info

    @app.route("/api/info")
    def api_info():
        cfg = workflow.config
        app_section = cfg.get("app", {})
        info = {
            "app_name": app_section.get("name"),
            "app_conf": app_section.get("conf"),
            "app_exp": app_section.get("exp"),
            "host": workflow.host.name,
            "scheduler": bool(workflow.jobmanager.with_scheduler),
            "workflow_dir": workflow.workflow_dir,
            "ncycles": len(workflow.cycles),
            "cycles": [str(c) for c in workflow.cycles],
            "task_tree": _serialize_task_tree(workflow.task_tree),
            "nmembers": workflow.nmembers,
        }
        return jsonify(info)

    # %% Config view

    @app.route("/api/config")
    def api_config():
        def sect_to_dict(section):
            result = {}
            for key, val in section.items():
                if hasattr(val, "items"):
                    sub = sect_to_dict(val)
                    if sub:
                        result[key] = sub
                elif isinstance(val, list):
                    result[key] = [str(v) for v in val]
                elif val is not None and str(val).strip():
                    result[key] = str(val)
            return result

        wf_cfg = sect_to_dict(workflow.config)
        tasks_cfg = {
            name: sect_to_dict(workflow.taskmanager.config[name]) for name in workflow.taskmanager.config
        }
        return jsonify({"workflow": wf_cfg, "tasks": tasks_cfg})

    # %% Status

    @app.route("/api/status")
    def api_status():
        rows = []
        for task_name, cycle, member in workflow:
            status = workflow.get_task_status(task_name, cycle, member)
            submdir = workflow.get_task_submission_dir(task_name, cycle, member, create=False)
            rows.append(
                {
                    "task": task_name,
                    "cycle": str(cycle) if cycle is not None else "",
                    "member": str(member) if member is not None else "",
                    "status": status.name,
                    "jobid": status.jobid if status.jobid else "",
                    "submdir": submdir,
                }
            )
        return jsonify(rows)

    # %% Artifacts

    @app.route("/api/artifacts")
    def api_artifacts():
        df = workflow.get_artifacts()
        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "TASK": str(row.get("TASK", "") or ""),
                    "ARTIFACT": str(row.get("ARTIFACT", "") or ""),
                    "PATH": str(row.get("PATH", "") or ""),
                    "EXISTS?": bool(row.get("EXISTS?", False)),
                }
            )
        return jsonify(records)

    # %% SSE log stream

    @app.route("/api/logs/stream")
    def api_logs_stream():
        def generate():
            q = sse_handler.subscribe()
            try:
                while True:
                    try:
                        data = q.get(timeout=30)
                        yield f"data: {data}\n\n"
                    except queue.Empty:
                        yield 'data: {"ping": true}\n\n'
            except GeneratorExit:
                pass
            finally:
                sse_handler.unsubscribe(q)

        return Response(
            generate(),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # %% Job file viewer

    @app.route("/api/job/file")
    def api_job_file():
        submdir = request.args.get("submdir", "")
        filetype = request.args.get("type", "out")
        if filetype not in ("out", "err", "sh"):
            abort(400, description="Invalid file type; must be out, err or sh")

        # Security: verify file is inside the workflow directory
        try:
            safe_root = os.path.realpath(workflow.workflow_dir) + os.sep
            file_path = os.path.realpath(os.path.join(submdir, f"job.{filetype}"))
            if not file_path.startswith(safe_root):
                abort(403, description="Access denied")
        except Exception:
            abort(403, description="Access denied")

        if not os.path.exists(file_path):
            return jsonify({"content": "", "exists": False})

        with open(file_path, errors="replace") as fh:
            content = fh.read()
        return jsonify({"content": content, "exists": True})

    # %% Run

    @app.route("/api/run", methods=["POST"])
    def api_run():
        if workflow.jobmanager.with_scheduler:
            return jsonify({"error": "Run not available for scheduler hosts"}), 403

        data = request.get_json(silent=True) or {}
        dry = bool(data.get("dry", False))
        force = bool(data.get("force", False))

        with _run_lock:
            if _run_state["running"]:
                return jsonify({"error": "Workflow is already running"}), 409
            _run_state["running"] = True
            _run_state["error"] = None

        def _runner():
            try:
                workflow.run(dry=dry, force=force)
            except Exception as exc:
                with _run_lock:
                    _run_state["error"] = str(exc)
            finally:
                with _run_lock:
                    _run_state["running"] = False

        threading.Thread(target=_runner, daemon=True).start()
        return jsonify({"started": True})

    @app.route("/api/run_status")
    def api_run_status():
        with _run_lock:
            return jsonify({"running": _run_state["running"], "error": _run_state["error"]})

    # %% Kill

    @app.route("/api/kill", methods=["POST"])
    def api_kill():
        if workflow.jobmanager.with_scheduler:
            return jsonify({"error": "Kill not available for scheduler hosts"}), 403

        data = request.get_json(silent=True) or {}
        jobid = data.get("jobid") or None
        task_name = data.get("task_name") or None
        cycle = data.get("cycle") or None

        try:
            workflow.kill(jobid=jobid, task_name=task_name, cycle=cycle)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        return jsonify({"killed": True})

    # %% Serve artifact files

    @app.route("/api/files")
    def api_files():
        path = request.args.get("path", "")
        try:
            safe_root = os.path.realpath(workflow.workflow_dir) + os.sep
            real_path = os.path.realpath(path)
            if not real_path.startswith(safe_root):
                abort(403, description="Access denied")
        except Exception:
            abort(403, description="Access denied")

        if not os.path.isfile(real_path):
            abort(404, description="File not found")

        return send_file(real_path)

    # %% Crontab helper

    def _woom_cmd():
        return shutil.which("woom") or "woom"

    def _crontab_lines():
        """Return (all_lines, returncode) from `crontab -l`."""
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        # rc=1 + "no crontab for user" means empty crontab — treat as empty
        if result.returncode != 0 and "no crontab" not in result.stderr.lower():
            raise RuntimeError(result.stderr.strip() or "crontab -l failed")
        return result.stdout.splitlines()

    def _crontab_marker():
        return f"woom:{workflow.workflow_dir}"

    @app.route("/api/crontab")
    def api_crontab_get():
        try:
            lines = _crontab_lines()
            marker = _crontab_marker()
            relevant = [ln for ln in lines if marker in ln]
            log_file = os.path.join(workflow.workflow_dir, "log", "woom_cron.log")
            return jsonify(
                {
                    "relevant": relevant,
                    "woom_cmd": _woom_cmd(),
                    "workflow_dir": workflow.workflow_dir,
                    "log_file": log_file,
                }
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/api/crontab", methods=["POST"])
    def api_crontab_post():
        data = request.get_json(silent=True) or {}
        action = data.get("action", "add")  # "add" | "remove"
        cron_expr = (data.get("cron_expr") or "").strip()

        if action == "add" and not cron_expr:
            return jsonify({"error": "cron_expr is required"}), 400

        try:
            lines = _crontab_lines()
            marker = _crontab_marker()
            # Remove any existing woom entries for this workflow
            lines = [ln for ln in lines if marker not in ln]

            new_line = None
            if action == "add":
                log_file = os.path.join(workflow.workflow_dir, "log", "woom_cron.log")
                new_line = (
                    f"{cron_expr} cd {workflow.workflow_dir} && "
                    f"{_woom_cmd()} run >> {log_file} 2>&1  # {marker}"
                )
                lines.append(new_line)

            new_crontab = "\n".join(lines)
            if new_crontab and not new_crontab.endswith("\n"):
                new_crontab += "\n"
            proc = subprocess.run(["crontab", "-"], input=new_crontab, capture_output=True, text=True)
            if proc.returncode != 0:
                return jsonify({"error": proc.stderr.strip() or "crontab update failed"}), 500
            return jsonify({"success": True, "line": new_line})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    # %% Stop server

    @app.route("/api/stop", methods=["POST"])
    def api_stop():
        """Gracefully stop the server — identical effect to Ctrl-C."""

        def _send_sigint():
            time.sleep(0.4)  # give Flask time to send the HTTP response
            os.kill(os.getpid(), signal.SIGINT)

        threading.Thread(target=_send_sigint, daemon=True).start()
        return jsonify({"stopping": True})

    return app


# %% Entry point


def run_monitor(workflow, host="127.0.0.1", port=5000, open_browser=True):
    """Start the Flask web monitor.

    Blocks until the user presses Ctrl-C or clicks the Stop button in the UI.
    Both paths send ``SIGINT`` to the main process — Flask's dev server handles
    this as a clean shutdown, so no special teardown is needed.

    Parameters
    ----------
    workflow : woom.workflow.Workflow
        Fully initialised workflow instance.
    host : str
        IP address to bind the server to.
    port : int
        TCP port for the server.
    open_browser : bool
        Open a browser tab after 1 s.
    """
    # Attach SSE handler *after* setup_logging so dictConfig doesn't remove it
    sse_handler = SSELogHandler()
    woom_logger = logging.getLogger("woom")
    woom_logger.addHandler(sse_handler)

    # Quieten Flask's noisy access log
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    app = create_monitor_app(workflow, sse_handler)

    if open_browser:
        url = f"http://{host}:{port}"

        def _open():
            import webbrowser

            webbrowser.open(url)

        t = threading.Timer(1.0, _open)
        t.daemon = True
        t.start()

    woom_logger.info(
        f"Starting woom monitor on http://{host}:{port}  (Ctrl-C or browser Stop button to quit)"
    )
    try:
        app.run(host=host, port=port, threaded=True, debug=False)
    except KeyboardInterrupt:
        pass
    finally:
        woom_logger.removeHandler(sse_handler)
        woom_logger.info("woom monitor stopped")
