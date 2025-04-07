.. _woom_show_rundirs:

:command:`woom show run_dirs`
=============================

Show the run directory of your workflow tasks.

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: show run_dirs

Example
-------

.. command-output:: woom show run_dirs
    :cwd: ../examples/academic/cycles
