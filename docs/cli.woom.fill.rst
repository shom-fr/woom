.. _cli.woom.fill:

:command:`woom fill`
=====================

Fill a Jinja template file with context variables and write it to a destination file.

This command allows you to manually fill template files using the workflow context. The templates are rendered with the same context variables that are available during workflow execution, including task-specific variables, cycle information, and member data.

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: fill

When to use
-----------

Use this command when you need to:

- Generate configuration files before running the workflow
- Fill templates outside of the automatic task submission process
- Test template rendering with specific context parameters
- Create files that depend on workflow context but aren't part of a task

The ``[[fill]]`` section in tasks configuration
------------------------------------------------

Instead of using this command manually, you can configure templates to be filled automatically during task submission by adding a ``[[fill]]`` section to your task configuration. See :ref:`cfgspecs.tasks` for details.

Examples
--------

Fill a template with default context::

    woom fill template.j2 output.txt

Fill a template for a specific task::

    woom fill namelist.nml.j2 run/namelist.nml --task-name run_model

Fill a template for a specific cycle and member::

    woom fill config.cfg.j2 run/config.cfg --task-name run_model --cycle 2020-01-01T00:00:00 --member 1

By default, task name, cycle and member default to the respective environment variables :envvar:`WOOM_TASK_NAME`, :envvar:`WOOM_CYCLE` and :envvar:`WOOM_MEMBER`.

Test template rendering without writing the file::

    woom fill template.j2 output.txt --dry-run
