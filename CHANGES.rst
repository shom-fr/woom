What's new
##########


Develop
=======

New features
------------
* App name is now inferred from ``workflow_dir`` when undefined.
* Add the ``nnodes``, ``ncpus``, ``nnodes`` and ``pmem`` submission options to tasks.
* Add the ``Context`` class that holds the input mappings for Jinja substitutions [:pull:`16`].
* Add the possibility to skip the artifact existence checking [:pull:`16`].
* Add the support for generating artifact paths with a registered function through a ``artifacts_generators`` extension [:pull:`16`].
* The ``Workflow.get_task_submission_dir``, ``Workflow.get_task_run_dir``, ``Workflow.get_task_artifacts`` can now return all possible values, i.e or all cycles and members as a dict or a flat list [:pull:`16`].
* Add a `template` and `blocking` option to task configuration [:pull:`18`].
* Add a `graceful` option to ``Workflow.kill`` to terminate jobs without killing them [:pull:`18`].
* Add a sentinel job that monitor jobs when using a scheduler [:pull:`18`].

Breaking changes
----------------
* Artifacts are now configured with one section per artifact [:pull:`16`].
* ``Workflow.get_submission_dir`` is renamed `Workflow.get_task_submission_dir``
* ``Workflow.get_run_dir`` is renamed ``Workflow.get_task_run_dir``.

Deprecations
------------

Bug fixes
---------

Documentation
-------------


2025.10.1
=========

Initial version.
