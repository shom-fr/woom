Use the templating system
=========================

About
-----

This example demonstrates how to customize woom's job script generation by extending the built-in templates.

Woom automatically generates job scripts for each task using default templates. This example shows how to override and extend these templates to add custom functionality.

The example customizes two templates:

**templates/job.sh** - Custom job script header:

- Uses ``{% extends "!job.sh" %}`` to inherit from the built-in job template (the ``!`` prefix refers to woom's internal templates)
- Overrides the ``{% block header %}`` section to add workflow metadata
- Uses ``{{ super() }}`` to include the parent template's content before adding custom lines
- Demonstrates accessing workflow context variables like ``app_name``, ``app_conf``, ``app_exp``, and ``task.name``

**templates/env.sh** - Custom environment setup:

- Extends the built-in environment template
- Adds a custom ``OMP_NUM_THREADS`` configuration in the ``{% block env_vars %}`` section
- Creates a new ``{% block custom %}`` with utility functions like ``log_message()``
- Shows how to add reusable bash functions for task scripts

This example is essential for users who need to customize job script headers, add environment setup, integrate with specific HPC systems, or add logging and debugging utilities.
