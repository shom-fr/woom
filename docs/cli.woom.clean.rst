.. _woom_clean:

:command:`woom clean`
=====================

Remove temporary files.

.. warning:: All jobs are automatically killed before trying to remove files.

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: clean

Examples
--------

Remove submission directorries::

    woom clean

Remove only log files and run directories::

    woom clean --without-submission-dirs --with-run-dirs --with-log-files

Remove extra files and directories too::

    woom clean out.txt our_dir/
