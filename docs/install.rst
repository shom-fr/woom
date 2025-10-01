Installation
============

.. highlight:: bash

Dependencies
------------

woom requires Python 3 or higher and depends on the following packages:

.. list-table::
   :widths: 10 90

   * - `colorlog <https://pypi.org/project/colorlog/>`_
     - Add colours to the output of Python's logging module.
   * - `configobj <https://configobj.readthedocs.io/en/latest/configobj.html>`_
     - :mod:`configobj` is a simple but powerful config file reader and writer: an ini file round-tripper.
   * - `jinja2 <https://jinja.palletsprojects.com/en/stable/>`_
     - :mod:`jinja2` is a fast, expressive, extensible templating engine. Special placeholders in the template allow writing code similar to Python syntax. Then the template is passed data to render the final document.
   * - `pandas <https://pandas.pydata.org/>`_
     - :mod:`pandas` is an open source, BSD-licensed library providing high-performance, easy-to-use data structures and data analysis tools for Python.
   * - `platformdirs <https://platformdirs.readthedocs.io/en/latest/>`_
     - :mod:`platformdirs` is a library to determine platform-specific system directories. This includes directories for placing cache files, user data, configuration, etc.
   * - `psutil <https://psutil.readthedocs.io/en/latest/>`_
     - :mod:`psutil` (python system and process utilities) is a cross-platform library for retrieving information on running processes and system utilization (CPU, memory, disks, network, sensors) in Python.
   * - `tabulate <https://github.com/astanin/python-tabulate>`_
     - Pretty-print tabular data in Python, a library and a command-line utility. Repository migrated from bitbucket.org/astanin/python-tabulate.


From sources
------------

Clone the repository::

    $ git clone https://github.com/shom-fr/woom.git

Install the build packages::

    $ pip install setuptools setuptools_scm

Run the installation command from the root directory::

    $ cd woom
    $ pip install .
