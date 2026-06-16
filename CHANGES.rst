What's new
##########


Develop
=======

New features
------------
* Add the ability to skip tasks: a task can be excluded from submission while
  remaining in the task tree, so downstream tasks can still reference its
  artifacts.  Skip a task permanently with ``skip = True`` in :file:`tasks.cfg`,
  or at runtime with ``skip = task1 task2`` in ``[stages]`` of
  :file:`workflow.cfg`, or via ``woom run --skip task1 task2`` on the CLI.
  Skipped tasks appear as ``SKIPPED`` (bold cyan) in ``woom show status`` [:pull:`35`].
* Add the ``woom monitor`` command: a lightweight Flask-based web UI for launching,
  monitoring, and stopping workflows from a browser.  Features include a live log
  stream (SSE), artifact browser with download links, an interactive crontab scheduler
  helper, and a dark/light theme toggle.
* The monitor Overview tab now displays an interactive visual task tree (clickable
  task nodes, cross-highlighted in the Jobs table) and collapsible workflow/tasks
  configuration panels.
* Add colors to ``woom show status``.
* App name is now inferred from ``workflow_dir`` when undefined.
* Add the ``nnodes``, ``ncpus``, ``nnodes`` and ``pmem`` submission options to tasks.
* Add the ``Context`` class that holds the input mappings for Jinja substitutions [:pull:`16`].
* Add the possibility to skip the artifact existence checking [:pull:`16`].
* Add the support for generating artifact paths with a registered function through a ``artifacts_generators`` extension [:pull:`16`].
* The ``Workflow.get_task_submission_dir``, ``Workflow.get_task_run_dir``, ``Workflow.get_task_artifacts`` can now return all possible values, i.e or all cycles and members as a dict or a flat list [:pull:`16`].
* Add a `template` and `blocking` option to task configuration [:pull:`18`].
* Add a `graceful` option to ``Workflow.kill`` to terminate jobs without killing them [:pull:`18`].
* Add a sentinel job that monitor jobs when using a scheduler [:pull:`18`].
* Add the capability to fill templates with a decated section in tasks and with the ``woom fill`` command [:pull:`22`].
* User paramaters specified in the workflow configuration can now contain sub-sections [:pull:`22`].
* PBS Pro: ``nnodes``, ``ncpus``, ``memory`` and ``pmem`` are now merged into a single
  ``-l select=N:ncpus=X:mpiprocs=X:mem=Y`` directive, fixing conflicts where PBS Pro rejects
  standalone ``-l ncpus=X``, ``-l mem=Y`` or ``-l pmem=Z`` alongside ``-l select=...``.
* PBS Pro: the ``extra`` submission option in ``[[submit]]`` now accepts a user-provided value
  (default: ``-koed``). Arbitrary keys added via ``__many__`` are also passed verbatim as raw
  qsub flags before the script path.
* Submission options ``nnodes``, ``ncpus`` and ``ngpus`` in ``[[submit]]`` now accept Jinja2
  template expressions (e.g. ``nnodes = {{ params.nnodes }}``); they are rendered at submission
  time using the task context.
* ``woom run --dry-run`` now logs a one-line summary per task at ``INFO`` level
  (``Fake submission: <task_path> → <qsub command>``); full detail remains available at ``DEBUG``.
* Switch CI and pre-commit to ruff [:pull:`34`]
* Add a ``horizon`` option to the ``[cycles]`` section: when ``as_intervals=False``, sets ``end_date = begin_date + horizon`` on each :class:`~woom.iters.Cycle`, making ``cycle_end_date`` and ``cycle_duration`` available in templates [:pull:`38`].

Bug fixes
---------
* Fix ``KeyError: 'cyan'`` in ``colorize()`` when displaying ``SKIPPED`` status on a TTY [:pull:`42`].
* Fix tasks with ``SUCCESS`` status being re-submitted on a subsequent
  ``woom run`` when all tasks had previously succeeded.  The condition
  ``status.name is JobStatus.SUCCESS`` compared a ``str`` to an ``Enum``
  member via ``is``, which is always ``False``; corrected to
  ``status is JobStatus.SUCCESS`` [:issue:`28`].
* Fix the sentinel PBS job being submitted unnecessarily on a second
  ``woom run`` when all tasks already succeeded.  ``get_task_status()``
  appended previously-submitted jobs to the job manager during status
  checks, making ``submit_sentinel()`` believe new jobs were pending.
  The sentinel is now only dispatched when at least one task was
  actually submitted in the current run [:issue:`28`].

Breaking changes
----------------
* Artifacts are now configured with one section per artifact [:pull:`16`].
* ``Workflow.get_submission_dir`` is renamed `Workflow.get_task_submission_dir``
* ``Workflow.get_run_dir`` is renamed ``Workflow.get_task_run_dir``.
* ``--update `` run option is renamed ``--force`` [:pull:`18`].
* User paramaters used in jinja rendering are now only accessible in the ``params`` variable [:pull:`22`].
* PBS Pro: the ``extra`` submission option format string changed from the hardcoded ``-keod`` to
  ``{}``; users who relied on the implicit ``-keod`` flag must now set ``extra = -koed`` explicitly
  (or keep the new default declared in ``tasks.ini``).

Deprecations
------------

Bug fixes
---------
* Fix job dependencies being lost between cycles when tasks are skipped via
  ``--skip`` or ``[stages] skip``: ``task_depend`` and ``sequence_depend`` are
  now preserved across skipped tasks instead of being reset to an empty list
  [:pull:`39`].
* SLURM: remove the hardcoded ``--exclusive`` flag from ``sbatch`` submissions;
  this option depends on the partition and should be set by the user in the
  ``[[submit]]`` section of their task when needed.
* Fix relative artifact paths being joined with an un-rendered ``run_dir`` Jinja
  template, causing the path to be resolved in the calling task's context instead
  of the artifact's own task context.
* Fix ``Workflow.get_artifacts`` iterating over characters of a path string for
  single-path artifacts instead of treating the string as one path.
* Fix ``Host.get_env`` that was ignoring ``raw_text`` and ``uv_venv`` configuration options.
* ``run_dir``, ``submission_dir``, ``script_path``, ``env`` must now be prefixed with ``task_`` in a jinja rendering.

Documentation
-------------


2025.10.1
=========

Initial version.
