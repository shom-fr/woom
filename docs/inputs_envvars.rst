.. _inputs_envvars:

Input environment variables
===========================

Woom specific new variables
---------------------------

You can use these variables in the scripts or programs you are calling from the job commands.

.. envvar:: WOOM_APP_NAME

    Example: ``'CROCO'``

.. envvar:: WOOM_APP_CONF

    Example: ``'MANGA'``

.. envvar:: WOOM_APP_EXP

    Example: ``'EXP23'``

.. envvar:: WOOM_APP_PATH

    Example: ``'CROCO/MANGA/EXP23'``

.. envvar:: WOOM_CYCLE_BEGIN_DATE

    Example: ``'2023-08-02 00:00:00'``

.. envvar:: WOOM_CYCLE_END_DATE

    Example: ``'2023-08-03 00:00:00'``

.. envvar:: WOOM_CYCLE_DURATION

    Example: ``'1 days 00:00:00'``

.. envvar:: WOOM_CYCLE_TOKEN

    Example: ``'2023-08-02T00:00:00-2023-08-03T00:00:00'``

.. envvar:: WOOM_CYCLE_LABEL

    Example: ``'2023-08-02T00:00:00 -> 2023-08-03T00:00:00 (1 days 00:00:00)'``

.. envvar:: WOOM_CYCLE_BEGIN_DATE_PREV

    Example: ``'2023-08-01 00:00:00'``

.. envvar:: WOOM_CYCLE_END_DATE_PREV

    Example: ``'2023-08-02 00:00:00'``

.. envvar:: WOOM_CYCLE_DURATION_PREV

    Example: ``'1 days 00:00:00'``

.. envvar:: WOOM_CYCLE_TOKEN_PREV

    Example: ``'2023-08-01T00:00:00-2023-08-02T00:00:00'``

.. envvar:: WOOM_CYCLE_LABEL_PREV

    Example: ``'2023-08-01T00:00:00 -> 2023-08-02T00:00:00 (1 days 00:00:00)'``

.. envvar:: WOOM_CYCLE_BEGIN_DATE_NEXT

    Example: ``'2023-08-03 00:00:00'``

.. envvar:: WOOM_CYCLE_END_DATE_NEXT

    Example: ``'2023-08-04 00:00:00'``

.. envvar:: WOOM_CYCLE_DURATION_NEXT

    Example: ``'1 days 00:00:00'``

.. envvar:: WOOM_CYCLE_TOKEN_NEXT

    Example: ``'2023-08-02T00:00:00-2023-08-03T00:00:00'``

.. envvar:: WOOM_CYCLE_LABEL_NEXT

    Example: ``'2023-08-03T00:00:00 -> 2023-08-04T00:00:00 (1 days 00:00:00)'``

.. envvar:: WOOM_LOG_DIR

    Example: ``'$WORKFLOW_DIR/jobs/2023-08-01T00:00:00-2023-08-02T00:00:00/ctask1/log'``

.. envvar:: WOOM_RUNDIR

    Example: ``'$HOME/woom/scratch/woom/CROCO/MANGA/EXP23/2023-08-01T00:00:00-2023-08-02T00:00:00/ctask1'``

.. envvar:: WOOM_TASK_NAME

    Example: ``'run_croco'``

.. envvar:: WOOM_SUBMISSION_DIR

    Example: ``'$WORKFLOW_DIR/jobs/CROCO/MANGA/EXP23/2023-08-01T00:00:00-2023-08-02T00:00:00/ctask1'``

.. envvar:: WOOM_TASK_PATH

    Example: ``'CROCO/MANGA/EXP23/2023-08-01T00:00:00-2023-08-02T00:00:00/ctask1'``

.. envvar:: WOOM_WORKFLOW_DIR

    Where the workflow is executed which is where lies the :file:`workflow.cfg` file.

Modified existing variables
---------------------------

.. envvar:: PATH

    ``"$WOOM_WORKFLOW_DIR/bin"`` is prepended to :envvar:`PATH`

.. envvar:: PYTHONPATH

    ``"$WOOM_WORKFLOW_DIR/lib/python"`` is prepended to :envvar:`PYTHONPATH`

.. envvar:: LIBRARY_PATH

    ``"$WOOM_WORKFLOW_DIR/lib"`` is prepended to :envvar:`LIBRARY_PATH`

.. envvar:: INCLUDE_PATH

    ``"$WOOM_WORKFLOW_DIR/include"`` is prepended to :envvar:`INCLUDE_PATH`
