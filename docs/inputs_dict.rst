.. _inputs_dict:

Input dictionary
================

This dictionary is used to fill the :ref:`templates` in order the finally generate the submitted job script.

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
   * - ``scratch_dir``
     - :class:`str`
     - Path to the scratch dir
     - ``"/scratch/username/woom"``
   * - ``task``
     - :class:`~woom.tasks.Task`
     - Task instance
     - ``"run_ibc"``
   * - ``task_path``
     - :class:`str`
     - `{app_path}/{cycle.token}/{task_name}/{member.label}`
     - ``"CROCO/MANGA/EXP/2020-01-01T12:00:00/run_ibc"``
   * - ``work_dir``
     - :class:`str`
     - Path to the work dir
     - ``"/work/username/woom"``
   * - ``workflow``
     - :class:`~woom.workflow.Workflow`
     - Workflow instance
     -
