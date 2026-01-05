.. _inputs_context:

Input context
=============

The input :class:`~woom.context.Context` is a dictionary
used to fill the :ref:`templates` to generate the submitted job script.

For instance, the following line, declared for instance in the :file:`tasks.cfg` file

.. code-block:: bash

    echo "the {{ app_conf }} model"

will be converted to

.. code-block:: bash

    echo "the CROCO model"

The minimal content of this dictionary is the following, in alphabetical order:

.. list-table::
   :widths: 10 30 30 30
   :header-rows: 1

   * - **Name**
     - **Type**
     - **Explanation**
     - **Example**
   * - ``app_conf``
     - :class:`str` or :class:`None`
     - App configuration name
     - ``"MANGA"``.
   * - ``app_exp``
     - :class:`str` or `None`
     - App experiment name
     - ``"RUN2025"``.
   * - ``app_name``
     - :class:`str` or `None`
     - App name
     - ``"CROCO"``.
   * - ``app_path``
     - :class:`str` or `None`
     - Merged version.
     - ``"CROCO/MANGA/RUN2025"``
   * - ``context``
     - :class:`~woom.context.Context`
     - Itself
     -
   * - ``cycle``
     - :class:`~woom.iters.Cycle` or `None`
     - Current cycle
     -
   * - ``cycles_begin_date``
     - :class:`~woom.util.WoomDate` or `None`
     - First date of cycles
     - ``Timestamp('2020-01-01 00:00:00')``
   * - ``cycles_end_date``
     - :class:`~woom.util.WoomDate` or `None`
     - Last date of cycles
     - ``Timestamp('2020-01-03 00:00:00')``
   * - ``cycles_freq``
     - :class:`~pandas.Timedelta` or `None`
     - Time step of cycles.
     - ``Timedelta('0 days 12:00:00')``
   * - ``cycles_round``
     - :class:`str` or `None`
     - Rounding precision
     - ``"D"``
   * - ``context_json``
     - :class:`str`
     - Path to the json file if this context
     - ``"{{ submission_dir }}/context.json"``
   * - ``env``
     - :class:`~woom.env.EnvConfig`
     - Task environment if task is not `None`
     -
   * - ``env_vars``
     - :class:`dict`
     - User and generated declared environment variables
     -
   * - ``host``
     - :class:`~woom.hosts.Host`
     - Host instance
     -
   * - ``hostmanager``
     - :class:`~woom.hosts.HostManager`
     - Host manager instance
     -
   * - ``logger``
     - :class:`logging.Logger`
     - The worflow logger
     -
   * - ``member``
     - :class:`~woom.iters.Member` or `None`
     - Current member
     -
   * - ``nmembers``
     - :class:`int`
     - Ensemble size
     - ``5``
   * - ``submission_dir``
     - :class:`str`
     - Job submission dir [#task]_
     - `"/home/username/woom/myworkflow"``
   * - ``run_dir``
     - :class:`str`
     - Job run dir [#task]_
     - `"/scrtach/username/woom/myapp/prolog/mytask"``
   * - ``paths``
     - :class:`dict`
     - Local workflow paths (bin, lib, lib/python...)
     -
   * - ``scratch_dir``
     - :class:`str`
     - Path to the scratch dir
     - ``"/scratch/username/woom"``
   * - ``task``
     - :class:`~woom.tasks.Task` or `None`
     - Task instance
     -
   * - ``task_name``
     - :class:`str` or `None`
     - Task instance
     - ``"run_ibc"``
   * - ``task_path``
     - :class:`str`
     - `{app_path}/{cycle.token}/{task_name}/{member.label}` [#task]_
     - ``"CROCO/MANGA/EXP/2020-01-01T12:00:00/run_ibc"``
   * - ``task_tree``
     - :class:`~woom.tasks.TastTree`
     - Task tree instance [#task]_
     - ``"run_ibc"``
   * - ``taskmanager``
     - :class:`~woom.tasks.TaskManager`
     - Task manager instance
     -
   * - ``os``
     - :mod:`os`
     - os module
     -
   * - ``params``
     - :class:`dict`
     - User-defined parameters from the ``[params]`` section of the workflow configuration. Access with ``params.<name>`` (e.g., ``{{ params.my_param }}``)
     -
   * - ``workflow``
     - :class:`~woom.workflow.Workflow`
     - Workflow instance
     -
   * - ``workflow_dir``
     - :class:`str`
     - Workflow dir
     - `"/home/username/woom/myworkflow"``
   * - ``<name>_dir``
     - :class:`str`
     - Directories declared in the host configuration
     -

.. [#task] Available only if ``task`` is not None
