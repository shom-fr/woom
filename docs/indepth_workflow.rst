.. _indepth.workflow:

Workflow Configuration In-Depth
################################

The workflow configuration file (:file:`workflow.cfg`) is the heart of your woom setup. It defines the overall structure, timing, and organization of your computational workflow.

Structure Overview
==================

A typical :file:`workflow.cfg` contains these main sections:

.. code-block:: ini

    [app]
    name = my_ocean_model
    conf = production
    exp = exp001

    [cycles]
    begin_date = 2020-01-01
    end_date = 2020-01-31
    freq = 1D
    indep = False

    [ensemble]
    size = 10
    tasks = run_model, postprocess

    [params]
    # Custom parameters available in templates

    [env_vars]
    # Environment variables set for all tasks

    [groups]
    # Task groups for parallel execution

    [stages]
        [[prolog]]
        setup = init_workspace

        [[cycles]]
        process = run_model, postprocess

        [[epilog]]
        cleanup = archive

Application Configuration
==========================

The ``[app]`` section identifies your workflow and creates a hierarchical directory structure.

Basic Configuration
-------------------

.. code-block:: ini

    [app]
    name = croco
    conf = benguela
    exp = test01

This creates the path structure: ``croco/benguela/test01/``

**Fields:**

- ``name``: Application name (defaults to workflow directory name if omitted)
- ``conf``: Configuration name (optional)
- ``exp``: Experiment name (optional)

**Directory Impact:**

The app path is used throughout woom:

- Job submission directories: :file:`jobs/{app_path}/task_name/`
- Run directories can reference: ``{{ app.name }}/{{ app.conf }}/{{ app.exp }}``
- Task paths include: ``{app_path}/{stage}/{task_name}``

Practical Example
-----------------

.. code-block:: ini

    [app]
    name = ocean_model
    conf = north_atlantic
    exp = spinup_2020

Results in:

- Submission dir: :file:`jobs/ocean_model/north_atlantic/spinup_2020/prolog/setup/`
- Available in templates as: ``{{ app.name }}``, ``{{ app.conf }}``, ``{{ app.exp }}``

Cycles Configuration
====================

Cycles allow tasks to repeat for different time periods. This is essential for time-stepping models and temporal workflows.

Date-Based Cycles
-----------------

.. code-block:: ini

    [cycles]
    begin_date = 2020-01-01T00:00:00
    end_date = 2020-01-05T00:00:00
    freq = 6H
    as_intervals = True
    indep = False

**Fields:**

- ``begin_date``: Start date (ISO 8601 format)
- ``end_date``: End date (optional, single cycle if omitted)
- ``freq``: Frequency between cycles (pandas offset string: '1D', '6H', '1M', etc.)
- ``ncycles``: Alternative to end_date - number of cycles to run
- ``as_intervals``: If True, cycles represent intervals [begin, end); if False, point-in-time
- ``indep``: If True, cycles run in parallel (independent); if False, sequential (each waits for previous)
- ``round``: Round dates to frequency (e.g., round='D' rounds to midnight)

**Example 1: Daily Cycles**

.. code-block:: ini

    [cycles]
    begin_date = 2020-01-01
    end_date = 2020-01-10
    freq = 1D
    as_intervals = True

Generates 9 cycles:
- 2020-01-01 to 2020-01-02
- 2020-01-02 to 2020-01-03
- ...
- 2020-01-09 to 2020-01-10

**Example 2: 6-Hour Intervals**

.. code-block:: ini

    [cycles]
    begin_date = 2020-01-01T00:00:00
    ncycles = 8
    freq = 6H
    as_intervals = True

Generates:

- 2020-01-01T00:00:00 to 2020-01-01T06:00:00
- 2020-01-01T06:00:00 to 2020-01-01T12:00:00
- ...
- 2020-01-02T18:00:00 to 2020-01-03T00:00:00

Cycle Dependencies
------------------

**Sequential Cycles (indep=False)**

.. code-block:: ini

    [cycles]
    indep = False

Each cycle waits for all tasks in the previous cycle to complete before starting. Use this for:

- Time-stepping models where each step depends on the previous
- Workflows where data from cycle N is needed in cycle N+1

**Independent Cycles (indep=True)**

.. code-block:: ini

    [cycles]
    indep = True

All cycles can run in parallel. Use this for:

- Embarrassingly parallel problems
- Independent ensemble members running different time periods
- Post-processing different time slices

No Cycles
---------

.. code-block:: ini

    [cycles]
    begin_date = 2020-01-01
    # No end_date, freq, or ncycles

Creates a single "cycle" with fixed date. Tasks in the cycles stage run once.

Forecast Cycles (``horizon``)
-----------------------------

The ``horizon`` option adds a forecast window to date-based cycles (``as_intervals = False``).
Without it, each cycle has only a ``begin_date`` and ``end_date`` is ``None``.
With ``horizon``, each cycle's ``end_date`` is set to ``begin_date + horizon``, making
``{{ cycle_end_date }}`` and ``{{ cycle_duration }}`` available in templates — without
changing the directory structure, which remains anchored to ``begin_date`` only.

``horizon`` accepts any pandas timedelta string (``5D``, ``12h``, ``1W``, …).
It is ignored when ``as_intervals = True`` (those cycles already have explicit end dates).

See :ref:`examples.academic.horizon` for a worked example.

Ensemble Configuration
======================

Ensembles allow running multiple realizations of tasks with different parameters.

Basic Ensemble
--------------

.. code-block:: ini

    [ensemble]
    size = 50
    tasks = run_model, analyze
    label = member

Creates 50 members (member001 to member050) that run ``run_model`` and ``analyze`` tasks.

**Fields:**

- ``size``: Number of ensemble members (None = no ensemble)
- ``tasks``: Which tasks should be ensembled (comma-separated list)
- ``skip``: Members to skip (e.g., skip = 1,5,10)
- ``label``: Label for members (default: "member")

Parameterized Ensembles
-----------------------

Use the ``iters`` subsection to create ensembles with varying parameters:

.. code-block:: ini

    [ensemble]
    size = 4
    tasks = run_model

        [[iters]]
        param1 = 0.1, 0.2, 0.3, 0.4
        param2 = high, high, low, low
        seed = 1234, 2345, 3456, 4567

Each member gets different parameter values:

- Member 1: param1=0.1, param2=high, seed=1234
- Member 2: param1=0.2, param2=high, seed=2345
- Member 3: param1=0.3, param2=low, seed=3456
- Member 4: param1=0.4, param2=low, seed=4567

Access in templates:

.. code-block:: jinja

    parameter_1 = {{ member.param1 }}
    parameter_2 = {{ member.param2 }}
    random_seed = {{ member.seed }}

Practical Example
-----------------

Sensitivity analysis with different wind forcing strengths:

.. code-block:: ini

    [ensemble]
    size = 5
    tasks = run_ocean
    label = scenario

        [[iters]]
        wind_scaling = 0.8, 0.9, 1.0, 1.1, 1.2
        description = weak, reduced, baseline, enhanced, strong

In your model configuration template:

.. code-block:: jinja

    ! Wind forcing scale factor
    wind_scale = {{ member.wind_scaling }}

    ! Run: {{ member.description }} winds

Workflow Stages
===============

Stages organize tasks into logical execution phases.

Stage Types
-----------

**Prolog**

Runs once at the workflow start. Use for:

- Creating directories
- Downloading initial data
- Compiling code
- Setting up databases

.. code-block:: ini

    [[prolog]]
    preparation = setup_dirs, download_forcings
    compilation = build_model

**Cycles**

Repeats for each cycle (if cycles are configured). Use for:

- Time-stepping simulations
- Iterative processing
- Temporal analysis

.. code-block:: ini

    [[cycles]]
    simulation = run_model
    analysis = compute_diagnostics, create_plots

**Epilog**

Runs once after all cycles complete. Use for:

- Final analysis
- Archiving results
- Cleanup
- Notifications

.. code-block:: ini

    [[epilog]]
    finalize = merge_outputs, create_summary
    archive = backup_results

Stage Sequences and Parallelism
--------------------------------

Within each stage, you can define multiple sequences (substages) that run sequentially:

.. code-block:: ini

    [[prolog]]
    # Sequence 1: runs first
    setup = create_dirs, copy_inputs

    # Sequence 2: runs after sequence 1
    prepare = compile_code, validate_inputs

    # Sequence 3: runs after sequence 2
    initialize = create_grid, setup_initial_conditions

Within each sequence, tasks can run in parallel by separating with commas:

.. code-block:: ini

    [[cycles]]
    # These three tasks run in parallel
    process = task1, task2, task3

Task Groups
-----------

Define reusable task groups in the ``[groups]`` section:

.. code-block:: ini

    [groups]
    preprocessing = clean_data, validate_data, transform_data
    core_model = initialize, run_timesteps, finalize
    postprocessing = extract_outputs, compute_stats

Use in stages:

.. code-block:: ini

    [[prolog]]
    prep = preprocessing

    [[cycles]]
    run = core_model, postprocessing

Skipping Tasks at Runtime
--------------------------

You can prevent specific tasks from being submitted without editing
:file:`tasks.cfg`, keeping them in the task tree so their artifact paths
remain accessible to downstream tasks.

**In :file:`workflow.cfg`** (persisted skip list for a given experiment):

.. code-block:: ini

    [stages]
        skip = preprocess, download_forcings

        [[prolog]]
        setup = preprocess, download_forcings, compile_model

        [[cycles]]
        run = run_model

Both ``preprocess`` and ``download_forcings`` will be silently bypassed on every
``woom run``, but ``run_model`` can still reference their artifact paths.

**On the command line** (one-off override):

.. code-block:: bash

    woom run --skip preprocess download_forcings

Multiple task names are space-separated.  CLI names are merged with any names
already listed in ``[stages] skip``, so you can combine both mechanisms.

**Behaviour summary:**

- Skipped tasks are **not submitted** and their submission directory is untouched
- They appear as ``SKIPPED`` (bold cyan) in ``woom show status``
- Their artifact paths are still displayed by ``woom show artifacts``
- Downstream tasks receive **no scheduler dependency** through a skipped slot
  (they can start immediately, assuming the artifacts already exist)
- ``--force`` does **not** override the skip; remove the task from the skip list
  to re-enable it

.. note::
   For a skip that is part of the task definition rather than a runtime choice,
   use the ``skip = True`` option directly in :file:`tasks.cfg`
   (see :ref:`indepth.tasks`).

Custom Parameters
=================

The ``[params]`` section defines custom variables available in all templates.

Global Parameters
-----------------

**Flat parameters** (most common):

.. code-block:: ini

    [params]
    domain = north_atlantic
    resolution = 10km
    grid_nx = 100
    grid_ny = 200
    timestep = 300

Access with underscores:

.. code-block:: jinja

    domain: {{ params.domain }}
    nx: {{ params.grid_nx }}
    timestep: {{ params.timestep }}

**Nested parameters** (using ConfigObj subsections):

.. code-block:: ini

    [params]
    domain = north_atlantic

        [[grid]]
        nx = 100
        ny = 200

        [[paths]]
        forcing_dir = /data/forcings
        output_dir = /scratch/outputs

Access with dots:

.. code-block:: jinja

    domain: {{ params.domain }}
    nx: {{ params.grid.nx }}
    forcing: {{ params.paths.forcing_dir }}

.. note::
   For simple workflows, use flat parameters with descriptive names (``grid_nx``, ``model_timestep``).
   Use nested sections when you have many related parameters to organize.

Host and Task-Specific Parameters
----------------------------------

Override parameters for specific hosts or tasks:

.. code-block:: ini

    [params]
    scratch_dir = /scratch/default

        [[hosts]]
            [[[local]]]
            scratch_dir = /tmp

            [[[hpc_cluster]]]
            scratch_dir = /scratch/users/$USER

        [[tasks]]
            [[[run_model]]]
            threads = 8
            memory = 32GB

Environment Variables
=====================

Set environment variables for all tasks:

.. code-block:: ini

    [env_vars]
    OMP_NUM_THREADS = 4
    MKL_NUM_THREADS = 4
    PYTHONUNBUFFERED = 1
    DATA_ROOT = /data/ocean

These are exported before task execution and available in templates.

Complete Example
================

Here's a comprehensive workflow configuration:

.. code-block:: ini

    # Ocean model workflow
    [app]
    name = ocean_model
    conf = tropical_pacific
    exp = hindcast_2020

    # Run daily cycles for January 2020
    [cycles]
    begin_date = 2020-01-01T00:00:00
    end_date = 2020-02-01T00:00:00
    freq = 1D
    as_intervals = True
    indep = False  # Sequential - each day needs previous

    # 10-member ensemble with different initial conditions
    [ensemble]
    size = 10
    tasks = run_ocean

        [[iters]]
        ic_perturbation = 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10

    # Custom parameters
    [params]
    model_timestep = 600
    output_frequency = 3600

        [[paths]]
        forcing = /data/forcings/era5
        bathymetry = /data/static/etopo1.nc

    # Environment for all tasks
    [env_vars]
    OMP_NUM_THREADS = 8
    DATA_DIR = /data/ocean

    # Reusable task groups
    [groups]
    analysis = compute_sst, compute_currents, create_plots

    # Workflow structure
    [stages]
        [[prolog]]
        setup = create_workspace, download_bathymetry
        prepare = generate_grid, compile_model

        [[cycles]]
        simulate = run_ocean
        postprocess = analysis

        [[epilog]]
        finalize = merge_all_outputs
        archive = backup_to_tape

Best Practices
==============

1. **Start Simple**: Begin with a basic workflow and add complexity (cycles, ensembles) incrementally

2. **Use Meaningful Names**: Application, configuration, and experiment names should be descriptive

3. **Plan Your Cycles**: Consider if cycles should be independent or sequential based on your science

4. **Organize Stages Logically**: Use prolog for setup, cycles for repeated work, epilog for finalization

5. **Document Parameters**: Add comments in your configuration explaining what parameters control

6. **Test Incrementally**: Test with a single cycle before running many, test with a few members before a large ensemble

7. **Use Groups**: Define task groups for commonly repeated task sequences

Common Patterns
===============

**Pattern 1: Simple Time-Stepping Model**

.. code-block:: ini

    [cycles]
    begin_date = 2020-01-01
    ncycles = 30
    freq = 1D
    indep = False

    [stages]
        [[cycles]]
        run = model_timestep

**Pattern 2: Embarrassingly Parallel Processing**

.. code-block:: ini

    [cycles]
    begin_date = 2020-01-01
    ncycles = 365
    freq = 1D
    indep = True  # All days can process in parallel

    [stages]
        [[cycles]]
        process = analyze_day

**Pattern 3: Ensemble Forecast**

.. code-block:: ini

    [ensemble]
    size = 50
    tasks = run_forecast

    [cycles]
    begin_date = 2020-01-01
    ncycles = 10
    freq = 1D
    indep = False

    [stages]
        [[cycles]]
        forecast = run_forecast

**Pattern 4: No Cycles, Just Stages**

.. code-block:: ini

    # No [cycles] section needed

    [stages]
        [[prolog]]
        prepare = download, preprocess

        [[epilog]]
        analyze = statistics, visualize

See Also
========

- :ref:`indepth.tasks` - Configure individual tasks
- :ref:`indepth.context` - Variables available in templates
- :ref:`cfgspecs.workflow` - Complete configuration reference
