.. _woom_monitor:

:command:`woom monitor`
=======================

Launch a lightweight web-based monitor for the current workflow.

The monitor provides a GitHub/GitLab-inspired single-page UI served on a local
Flask server.  It lets you inspect the workflow, launch and kill jobs, browse
live log output, view artifacts, and schedule recurring runs via cron — all
from a browser.

.. note::

    Flask must be installed to use this command::

        pip install flask
        # or, using the optional dependency group:
        pip install "woom[monitor]"

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: monitor

UI tabs
-------

**Overview**
    Summary cards (host, scheduler, number of cycles, ensemble members,
    workflow directory), a text rendering of the task tree, cycle tags, and a
    live status-count summary.

**Jobs**
    Auto-refreshing table (every 3 s) showing the status, job ID, task, cycle,
    member, and submission directory for every task instance.
    Action buttons: *Run*, *Dry run*, *Force run*, *Kill all*.
    Per-row *Kill* (running jobs only) and *Logs* buttons.

    .. note::

        Run and Kill actions are disabled for scheduler hosts (Slurm / PBS).
        A warning banner is shown in that case.

**Logs**
    Left panel: live woom log stream over SSE, colour-coded by level
    (DEBUG, INFO, WARNING, ERROR).  Includes a history of the last 500
    records for late-connecting clients.
    Right panel: job file browser — select a task and view its
    ``job.out``, ``job.err``, or ``job.sh``.

**Artifacts**
    Table of all artifact paths declared by the workflow tasks, with an
    existence indicator (✓ / ✗) and a download link for existing files.

**Schedule**
    Crontab helper.  Shows any existing cron entry for this workflow,
    provides frequency presets (every 5 min, 15 min, 30 min, 1 h, 6 h,
    daily), lets you edit the crontab expression manually, previews the full
    generated line, and writes it to the user crontab with *Apply*.
    The generated line appends to ``log/woom_cron.log`` inside the
    workflow directory.

Shutting down the server
------------------------

Press **Ctrl-C** in the terminal, or click the **■ Shutdown** button in the
browser.  Both send ``SIGINT`` to the Flask process for a clean shutdown.

Dark / light theme
------------------

The UI respects the system ``prefers-color-scheme`` setting by default.
A toggle button (☾ / ☀) in the header lets you override the theme manually;
the choice is persisted in ``localStorage``.

Examples
--------

Start the monitor on the default address and port::

    cd /path/to/workflow
    woom monitor

Bind to all interfaces on a custom port, without opening a browser::

    woom monitor --bind 0.0.0.0 --port 8080 --no-browser

Use a custom workflow configuration::

    woom monitor --workflow-cfg custom.cfg --host myhost
