Installation
============

.. highlight:: bash

Dependencies
------------

shoot requires ``python>3`` and depends on the following packages:

.. list-table::
   :widths: 10 90

   * - `matplotlib <https://matplotlib.org/>`_
     - Matplotlib is a comprehensive library for creating static, animated,
       and interactive visualizations in Python.
   * - `numba <https://numba.pydata.org/>`_
     - A high performance python compiler.
   * - `scipy <https://www.scipy.org/scipylib/index.html>`_
     - Scipy provides many user-friendly and efficient numerical routines,
       such as routines for numerical integration, interpolation,
       optimization, linear algebra, and statistics.
   * - `xarray <http://xarray.pydata.org/en/stable/>`_
     - xarray is an open source project and Python package that makes working
       with labelled multi-dimensional arrays simple, efficient, and fun!
   * - `xoa <https://xoa.readthedocs.io/en/develop/>`_
     - xoa helps analyzing ocean fields



From sources
------------

Clone the repository::

    $ git clone https://gitlab.com/GitShom/STM/shoot

Run the installation command from the root directory::

    $ cd shoot
    $ pip install .
