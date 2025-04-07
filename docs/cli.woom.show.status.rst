.. _woom_show_status:

:command:`woom show status`
===========================

Show the status of jobs that were submitted with the workflow.

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: show status

Example
-------

.. command-output:: woom show status
    :cwd: ../examples/academic/all_stages
