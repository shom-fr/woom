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

import hmac
import json
import logging
import os
import queue
import re
import secrets
import shlex
import shutil
import signal
import subprocess
import threading
import time
from collections import deque

from flask import Flask, Response, abort, jsonify, redirect, render_template, request, send_file

try:
    from .. import __version__ as _woom_version
except Exception:
    _woom_version = "unknown"

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

    Stage order is always forced to prolog → cycles → epilog so that
    ConfigObj's internal reordering does not affect the pipeline view.
    """
    _STAGE_ORDER = ["prolog", "cycles", "epilog"]
    stages = [s for s in _STAGE_ORDER if s in task_tree] + [s for s in task_tree if s not in _STAGE_ORDER]
    result = {}
    for stage in stages:
        sequences = task_tree[stage]
        if not sequences:
            result[stage] = {}
            continue
        result[stage] = {}
        for seq_name, groups in sequences.items():
            result[stage][seq_name] = [list(g) for g in groups]
    return result


#: Environment variable used to keep the access token across restarts
TOKEN_ENV_VAR = "WOOM_MONITOR_TOKEN"

#: Name of the HTTP header that may carry the access token
TOKEN_HEADER = "X-Woom-Token"

_CRON_FIELD_RE = re.compile(r"^[0-9A-Za-z*/,\-]+$")


def is_valid_cron_expr(expr):
    """Check that `expr` is a plain 5-field crontab time specification

    Only digits, letters (month and day names) and the ``* / , -``
    characters are accepted, so that no shell command can be injected
    in the crontab line.
    """
    fields = expr.split()
    return len(fields) == 5 and all(_CRON_FIELD_RE.match(field) for field in fields)


def _cookie_name():
    # Cookies are not isolated by port: include it to support several monitors
    return "woom_monitor_token_" + re.sub(r"\W", "_", request.host)


# %% Flask application factory


def create_monitor_app(workflow, sse_handler, restart_flag=None, token=None):
    """Create and return the Flask application.

    Templates are loaded from ``woom/monitor/templates/``;
    static assets from ``woom/monitor/static/`` (served at ``/static/``).

    All routes except static assets require the access `token`.
    It is first given as a ``token`` query parameter of the index page,
    which stores it in a strict same-site HTTP-only cookie.
    It may also be passed with the ``X-Woom-Token`` header.
    POST requests must furthermore have a JSON content type,
    which prevents cross-site form submissions.

    Parameters
    ----------
    workflow : woom.workflow.Workflow
        Fully initialised workflow instance.
    sse_handler : SSELogHandler
        SSE log handler (already attached to ``logging.getLogger("woom")``).
    restart_flag : threading.Event, None
        Event set when a restart is requested.
    token : str, None
        Access token. A random one is generated when not provided,
        and is available as ``app.config["WOOM_MONITOR_TOKEN"]``.

    Returns
    -------
    flask.Flask
    """
    # Flask resolves template_folder / static_folder relative to __file__
    # (__file__ == woom/monitor/__init__.py → package root is woom/monitor/)
    app = Flask(__name__)
    if not token:
        token = secrets.token_urlsafe(32)
    app.config["WOOM_MONITOR_TOKEN"] = token

    def _check_token(value):
        return bool(value) and hmac.compare_digest(str(value), token)

    @app.before_request
    def _authenticate():
        if request.endpoint == "static":
            return
        # First access from the URL printed in the terminal
        if request.path == "/" and _check_token(request.args.get("token")):
            resp = redirect("/")
            resp.set_cookie(_cookie_name(), token, httponly=True, samesite="Strict")
            return resp
        if not (
            _check_token(request.cookies.get(_cookie_name()))
            or _check_token(request.headers.get(TOKEN_HEADER))
        ):
            abort(
                403,
                description="Access denied: open the monitor with the URL that contains "
                "the access token, as printed in the terminal by woom monitor",
            )
        if request.method == "POST" and not request.is_json:
            abort(415, description="POST requests must have a JSON content type")

    # Preserve JSON key order — Flask ≤ 2.1 sorts keys alphabetically by
    # default, which would scramble prolog/cycles/epilog ordering in task_tree.
    try:
        app.json.sort_keys = False  # Flask ≥ 2.2
    except AttributeError:
        app.config["JSON_SORT_KEYS"] = False  # Flask < 2.2

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
            "woom_version": _woom_version,
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

    # %% Clean

    @app.route("/api/clean", methods=["POST"])
    def api_clean():
        data = request.get_json(silent=True) or {}
        submission_dirs = bool(data.get("submission_dirs", True))
        log_files = bool(data.get("log_files", True))
        run_dirs = bool(data.get("run_dirs", False))
        artifacts = bool(data.get("artifacts", False))
        extra_files = data.get("extra_files") or None
        dry = bool(data.get("dry", True))

        try:
            workflow.clean(
                submission_dirs=submission_dirs,
                log_files=log_files,
                run_dirs=run_dirs,
                artifacts=artifacts,
                extra_files=extra_files,
                dry=dry,
            )
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        return jsonify({"cleaned": True})

    # %% Serve artifact files

    @app.route("/api/files")
    def api_files():
        path = request.args.get("path", "")
        try:
            real_path = os.path.realpath(path)
        except Exception:
            abort(403, description="Access denied")

        # Allow files inside the workflow directory …
        safe_root = os.path.realpath(workflow.workflow_dir) + os.sep
        allowed = real_path.startswith(safe_root)

        # … or files that are declared artifacts of this workflow
        if not allowed:
            try:
                df = workflow.get_artifacts()
                artifact_paths = {os.path.realpath(p) for p in df["PATH"].dropna().tolist() if p}
                allowed = real_path in artifact_paths
            except Exception:
                pass

        if not allowed:
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

        if action not in ("add", "remove"):
            return jsonify({"error": "action must be add or remove"}), 400
        if action == "add" and not cron_expr:
            return jsonify({"error": "cron_expr is required"}), 400
        if action == "add" and not is_valid_cron_expr(cron_expr):
            return jsonify({"error": f"Invalid crontab expression: {cron_expr!r}"}), 400

        try:
            lines = _crontab_lines()
            marker = _crontab_marker()
            # Remove any existing woom entries for this workflow
            lines = [ln for ln in lines if marker not in ln]

            new_line = None
            if action == "add":
                log_file = os.path.join(workflow.workflow_dir, "log", "woom_cron.log")
                new_line = (
                    f"{cron_expr} cd {shlex.quote(workflow.workflow_dir)} && "
                    f"{shlex.quote(_woom_cmd())} run >> {shlex.quote(log_file)} 2>&1  # {marker}"
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

    # %% Stop / restart server

    @app.route("/api/stop", methods=["POST"])
    def api_stop():
        """Gracefully stop the server — identical effect to Ctrl-C."""

        def _send_sigint():
            time.sleep(0.4)  # give Flask time to send the HTTP response
            os.kill(os.getpid(), signal.SIGINT)

        threading.Thread(target=_send_sigint, daemon=True).start()
        return jsonify({"stopping": True})

    @app.route("/api/restart", methods=["POST"])
    def api_restart():
        """Restart the monitor by re-executing the current process.

        Sets the restart flag then sends SIGINT — exactly like /api/stop but
        with the flag set so that ``run_monitor`` knows to re-exec after the
        port has been released.
        """
        if restart_flag is not None:
            restart_flag.set()

        def _send_sigint():
            time.sleep(0.4)
            os.kill(os.getpid(), signal.SIGINT)

        threading.Thread(target=_send_sigint, daemon=True).start()
        return jsonify({"restarting": True})

    return app


# %% Entry point


def run_monitor(workflow, host="127.0.0.1", port=5000, open_browser=True):
    """Start the Flask web monitor.

    Blocks until the user presses Ctrl-C or clicks the Stop button in the UI.
    Both paths send ``SIGINT`` to the main process — Flask's dev server handles
    this as a clean shutdown, so no special teardown is needed.

    A random access token is generated, or taken from the
    ``WOOM_MONITOR_TOKEN`` environment variable,
    and the URL that contains it is printed on the standard output.

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

    # Keep the same token after a restart so that open pages keep working
    token = os.environ.get(TOKEN_ENV_VAR) or secrets.token_urlsafe(32)
    os.environ[TOKEN_ENV_VAR] = token

    restart_flag = threading.Event()
    app = create_monitor_app(workflow, sse_handler, restart_flag, token=token)

    url = f"http://{host}:{port}/?token={token}"
    if open_browser:

        def _open():
            import webbrowser

            webbrowser.open(url)

        t = threading.Timer(1.0, _open)
        t.daemon = True
        t.start()

    woom_logger.info(
        f"Starting woom monitor on http://{host}:{port}  (Ctrl-C or browser Stop button to quit)"
    )
    # The token is printed but not logged to keep it out of log files
    print(f"Open the woom monitor at: {url}", flush=True)
    try:
        app.run(host=host, port=port, threaded=True, debug=False)
    except KeyboardInterrupt:
        pass
    finally:
        woom_logger.removeHandler(sse_handler)

    # The port is now released.  If the restart flag was set, re-exec.
    if restart_flag.is_set():
        import sys

        woom_logger.info("woom monitor restarting…")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    else:
        woom_logger.info("woom monitor stopped")
