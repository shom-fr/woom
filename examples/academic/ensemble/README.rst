Running an ensemble
===================

About
-----

Run a simple task through an ensemble and cycles.

We extend the capability of woom in two ways:

#. We add a Jinja filter named ``member2letter`` by adding the
   :file:`ext/jinja_filters.py` in the workflow directory.
   This allows to add to convert the member id to a letter with
   this templating on the task command line ``{{ member|member2letter }}``.
#. We add a workflow configuration specification file named :file:`workflow.ini`
   that is merged with the default one to
   help converting the option named ``ks`` to a list of lognormal random numbers.
   The conversion function is declared in the :file:`ext/validator_functions.py`.
   It takes as arguments a mean, a standard deviation and a size.
