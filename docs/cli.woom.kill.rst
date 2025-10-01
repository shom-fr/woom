.. _woom_kill:

:command:`woom kill`
====================

Kill jobs hat were submitted when running the current workflow.

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: kill

Examples
--------

Kill all jobs::

    woom kill

Kill only a job with a specific id::

    woom kill 1251

Kill a job bounded to a specific task::

    woom kill --task download_data

Kill all job that belongs to a specific cycle::

    woom kill --cycle prolog
    woom kill --cycle 2020-01-01T12:00:00-2020-01-02T00:00:00
