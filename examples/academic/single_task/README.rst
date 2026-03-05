A single task with command line formatting
==========================================

About
-----

This is the simplest woom example, demonstrating the fundamental concepts of workflow configuration.

The example runs a single task in the prolog stage that showcases:

- **Basic workflow structure**: Application configuration, parameters, and stages
- **Command line templating**: Using Jinja2 syntax to insert parameters into task commands
- **Jinja2 filters**: Applying transformations like ``upper`` and ``strftime`` to format values
- **Custom filters**: Using the ``replicate_option`` filter to generate repeated command-line options
- **Parameter override**: Task-specific parameters that override global values
- **Environment variables**: Setting custom environment variables for tasks

This example is ideal for getting started with woom and understanding how configuration files, parameters, and templating work together.
