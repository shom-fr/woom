.. _from_python:

From python
===========

Instead of calling the :ref:`commandline interface <cli>`, you can run your workflow from python.
Here is a minimal list of operations you can do.
Have a look to the :mod:`woom.cli` module for more info.

Initializations
---------------

.. highlight:: python

Import modules::

    import logging
    import woom.log as wlog
    import woom.ext as wext
    import woom.tasks as wtasks
    import woom.hosts as whosts
    import woom.workflow as wworkflow

Initialize the **logger**::

    wlog.setup_logger(console_level="debug", to_file="woom.log")
    logger = logging.getLogger("woom")

Setup the workflow
------------------


Load the **extension files** with :func:`~woom.ext.load_jinja_filters` and :func:`~woom.ext.load_validator_functions`::

    wext.load_jinja_filters("ext/jinja_filters.py") # Jinja filters
    wext.load_validator_functions("ext/validator_functions.py") # Configobj validator functions

Alternatively, if your extension files are in the :file:`ext/` directory, you can use :func:`~woom.ext.load_extensions`::

    wext.load_extensions(".")

Initialize the :class:`host manager <woom.hosts.HostManager>`::

    hostmanager = whosts.HostManager()
    hostmanager.load_config("hosts.cfg")

Set your :class:`host <woom.hosts.Host>`::

    host = hostmanager.get_host("datarmor")

Alternatively, you can infer your **host** thanks to the ``patterns`` configuration option if set::

    host = hostmanager.infer_host()

Initialize the :class:`task manager <woom.tasks.TaskManager>`::

    taskmanager = wtasks.TaskManager(host)
    taskmanager.load_config("tasks.cfg")

Finally, initialize the :class:`workflow <woom.workflow.Workflow>`::

    workflow = wworkflow.Workflow("workflow.cfg", taskmanager)

Operate
-------

To print an **overview**, call :meth:`~woom.workflow.Workflow.get_overview`::

    workflow.show_overview()

If it's ok, **run** the workflow with :meth:`~woom.workflow.Workflow.run`.
Do it first in fake mode so that it will tell what will be done without doing it::

    workflow.run(dry=True)

The run it for real::

    workflow.run()

If you just want to re-run tasks that have already run, use the ``update`` keyword::

    workflow.run(update=True)

To **check the status** of all workflow tasks and their associated jobs, use :meth:`~woom.workflow.Workflow.show_status`::

    workflow.show_status()

To show the status of only running jobs, use the ``running`` keyword::

    workflow.show_status(running=True)

If you wan to **kill** all your jobs, call :meth:`~woom.workflow.Workflow.kill`::

    workflow.kill()

You can fine tune which jobs you want to kill::

    workflow.kill("1256")                                          # by job id
    workflow.kill(task_name="mytask")                              # by task
    workflow.kill(cycle="2020-01-01T00:00:00-2020-01-01T06:00:00") # by cycle
    workflow.kill(member=1)                                        # by ensemble member id
