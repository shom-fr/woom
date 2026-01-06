What's new
##########


Develop
=======

New features
------------
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

Breaking changes
----------------
* Artifacts are now configured with one section per artifact [:pull:`16`].
* ``Workflow.get_submission_dir`` is renamed `Workflow.get_task_submission_dir``
* ``Workflow.get_run_dir`` is renamed ``Workflow.get_task_run_dir``.
* ``--update `` run option is renamed ``--force`` [:pull:`18`].
* User paramaters used in jinja rendering are now only accessible in the ``params`` variable [:pull:`22`].

Deprecations
------------

Bug fixes
---------
* Fix ``Host.get_env`` that was ignoring ``raw_text`` and ``uv_venv`` configuration options.
* ``run_dir``, ``submission_dir``, ``script_path``, ``env`` must now be prefixed with ``task_`` in a jinja rendering.

Documentation
-------------


2025.10.1
=========

Initial version.
