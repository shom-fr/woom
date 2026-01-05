.. _indepth:

In-Depth Guide
##############

This section provides comprehensive, detailed explanations of woom's core features and concepts. Each guide includes practical examples, best practices, and advanced usage patterns.

.. toctree::
   :maxdepth: 2

   indepth_workflow
   indepth_tasks
   indepth_hosts
   indepth_context
   indepth_templating
   indepth_artifacts

Overview
========

Woom is built around three main configuration files:

1. **workflow.cfg** - Defines your workflow structure, cycles, ensemble, and stages
2. **tasks.cfg** - Defines individual tasks with their commands, environments, and artifacts
3. **hosts.cfg** - Defines execution environments (local, HPC clusters) with schedulers and queues

These files work together with a powerful **templating system** that allows you to:

- Generate dynamic configuration files using Jinja2 templates
- Fill template files with context variables (task name, cycle dates, member IDs, etc.)
- Create portable workflows that adapt to different environments

Core Concepts
=============

Workflows
---------

A workflow orchestrates the execution of multiple tasks across different stages:

- **Prolog**: Setup tasks that run once at the beginning
- **Cycles**: Tasks that repeat for different time periods or iterations
- **Epilog**: Cleanup tasks that run once at the end

Tasks
-----

Tasks are the fundamental units of work in woom. Each task:

- Executes a command line in a specific environment
- Can produce artifacts (output files) that are tracked
- Can depend on other tasks through the workflow structure
- Can fill template files before execution

Hosts
-----

Hosts define where and how tasks execute:

- Local execution with background processes
- HPC schedulers (SLURM, PBS Pro)
- Custom environments with modules and virtual environments
- Queue configurations for different resource requirements

Context and Variables
---------------------

The context provides variables available for templating:

- Workflow information (paths, application name)
- Task details (name, run directory)
- Cycle information (dates, intervals, tokens)
- Ensemble member data
- Custom parameters from configuration

Getting Started
===============

For a complete understanding of woom, we recommend reading the guides in this order:

1. :ref:`indepth.workflow` - Understand workflow structure and stages
2. :ref:`indepth.tasks` - Learn how to define and configure tasks
3. :ref:`indepth.hosts` - Set up execution environments
4. :ref:`indepth.context` - Master the variable system
5. :ref:`indepth.templating` - Use Jinja2 templates for dynamic content
6. :ref:`indepth.artifacts` - Track and validate output files

Each guide builds on concepts from previous sections while remaining self-contained enough to use as a reference.
