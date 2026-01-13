Running an ensemble
===================

About
-----

This example demonstrates how to run ensemble simulations combined with time cycles, along with advanced extension capabilities.

The workflow runs a task across multiple ensemble members and cycles, showcasing woom's ability to handle both temporal and ensemble dimensions simultaneously.

The example demonstrates two powerful extension mechanisms:

**1. Custom Jinja2 filter** (:file:`ext/jinja_filters.py`):

- Adds a ``member2letter`` filter that converts member IDs to letters (1→A, 2→B, etc.)
- Shows how to extend woom's templating system with domain-specific transformations
- Used in task command lines: ``{{ member|member2letter }}``
- Illustrates how to add custom formatting for ensemble parameters

**2. Custom validator function** (:file:`ext/validator_functions.py`):

- Extends workflow configuration validation with a custom ``ks`` parameter
- Generates lognormal random numbers from mean, standard deviation, and size arguments
- Demonstrates how to add complex parameter types beyond woom's built-in types
- Defined via a custom :file:`workflow.ini` specification file that merges with woom's defaults

This example is ideal for users running ensemble forecasts, sensitivity studies, or Monte Carlo simulations who need to extend woom's capabilities with custom functionality.
