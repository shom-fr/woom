Commandline interface
#####################

.. highlight:: bash

Here is the command line interface of woom.
All these commands are expected to be executed with at least valid workflow and tasks configuration files.
The respectively defaults to :file:`workflow.cfg` and :file:`tasks.cfg` in the current directory.

:command:`woom`
==============

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :nosubcommands:


:command:`woom overview`
========================

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: overview



:command:`woom run`
===================

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: run


:command:`woom status`
======================

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: status





:command:`woom kill`
======================

.. argparse::
    :module: woom.cli
    :func: get_parser
    :prog: woom
    :path: kill
    
Examples
--------

Kill all jobs::

    woom kill

Kill only a job with a specific id:

    woom status 1251

Kill a job bounded to a specific task:

    woom status --task download_data

Kill all job that belongs to a specific cycle:

    woom status --cycle prolog
    woom status --cycle 20200000000000000000000
    