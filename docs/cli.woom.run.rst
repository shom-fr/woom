.. _woom_run:

:command:`woom run`
===================

Submit the current workflow.

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: run

Example
-------

.. command-output:: woom run --log-no-color
    :cwd: ../examples/academic/all_stages
