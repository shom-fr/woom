.. _ext:

Extending woom
##############

One can extend the capabilities of woom by supplying specific files.


Jinja filters
=============

Jina is used to perform string substitutions to generate the task scripts to be submitted.
This process support filtering that transforms you data after converting it to string.
Jinja builtin filters are available `here <https://jinja.palletsprojects.com/en/stable/templates/#builtin-filters>`_.
Woom provides a few filters in the :mod:`woom.render` module.

You can add you own filter functions by following the procedure:

1. Create a python file named :file:`jinja_filters.py` located in the :file:`ext/` folder of your workflow directory.
2. Declare in this file all the jinja filter functions you want.
3. Declare a dictionary named :attr:`JINJA_FILTERS` to register your functions.

Example:

.. literalinclude:: ../examples/academic/ensemble/ext/jinja_filters.py
    :caption: :file:`examples/academic/ensemble/ext/jinja_filters.py`
    :start-at: import

If present, this file is loaded at workflow setup by the :func:`woom.ext.load_extensions` function.

Configobj specifications
========================

Parameters that are passed to the workflow configuration in the ``[params]`` are 
interpreted as strings by default.
You can change this behavior by changing adding your own specification file,
and optionally by providing new validator functions that convert text to 
the desired type.

Workflow configuration specifications
-------------------------------------


Just pass the commandline option `--workflow-ini` to give a configuration specification file that
will be merged with the :ref:`default one <cfgspecs.workflow>`.
For info about how to write such file, refer to 
`the doc <https://configobj.readthedocs.io/en/latest/configobj.html#configspec>`_.
An option specification can refer to an existing validator function like ``boolean`` or a new one (see next section).

Example:

.. literalinclude:: ../examples/academic/ensemble/workflow.ini
    :caption: :file:`examples/academic/ensemble/workflow.ini`
    :language: ini

Validator functions
-------------------

You can add new validator functions by following the procedure:

1. Create a python file named :file:`validator_functions.py` located in the :file:`ext/` folder of your workflow directory.
2. Declare in this file all the validator functions you want.
3. Declare a dictionary named :attr:`VALIDATOR_FUNCTIONS` to register your functions.

Example:

.. literalinclude:: ../examples/academic/ensemble/ext/validator_functions.py
    :caption: :file:`examples/academic/ensemble/ext/validator_functions.py`
    :start-at: import

If present, this file is loaded at workflow setup by the :func:`woom.ext.load_extensions` function.
