.. _indepth.tasks:

Task Configuration In-Depth
############################

Tasks are the fundamental units of work in woom. The :file:`tasks.cfg` file defines what each task does, where it runs, what resources it needs, and what outputs it produces.

Structure Overview
==================

A task configuration consists of several sections:

.. code-block:: ini

    [task_name]
        [[content]]
        commandline = echo "Hello World"
        run_dir = {{ scratch_dir }}/run
        env = myenv
        template = job.sh

        [[artifacts]]
            [[[output_file]]]
            path = output.txt
            check = True

        [[fill]]
            [[[config]]]
            template = model.cfg.j2
            destination = {{ task_run_dir }}/model.cfg

        [[submit]]
        queue = normal
        nnodes = 1
        ncpus = 16
        time = 02:00:00
        blocking = True

Task Content
============

The ``[[content]]`` section defines what the task executes and where.

Command Line
------------

The ``commandline`` specifies what to execute:

.. code-block:: ini

    [[content]]
    commandline = ./my_model input.nml

**Simple Commands:**

.. code-block:: ini

    commandline = python analyze_data.py

**Multiple Commands:**

Use shell syntax (&&, ;, ||):

.. code-block:: ini

    commandline = cd {{ task_run_dir }} && ./prepare.sh && ./run_model.exe

**Multi-Line Commands:**

.. code-block:: ini

    commandline = '''
        set -e
        export DATA_DIR=/scratch/data
        python preprocess.py
        mpirun -n 128 ./ocean_model
        python postprocess.py
        '''

**With Template Variables:**

.. code-block:: ini

    commandline = mpirun -n {{ params.nprocs }} ./model -i {{ cycle.begin_date }}

Run Directory
-------------

The ``run_dir`` is where the command executes:

.. code-block:: ini

    [[content]]
    run_dir = /scratch/{{ app.name }}/{{ task_path }}

**Special Values:**

- ``current`` - Use current working directory
- ``None`` or empty - No cd before execution

**Common Patterns:**

.. code-block:: ini

    # Unique per task, cycle, and member
    run_dir = {{ scratch_dir }}/{{ task_path }}

    # Shared run directory
    run_dir = {{ workflow_dir }}/run

    # Organized by date
    run_dir = /scratch/runs/{{ cycle.date }}

**Available Variables:**

- ``{{ scratch_dir }}`` - Scratch directory from host config
- ``{{ workflow_dir }}`` - Where workflow.cfg is located
- ``{{ task_path }}`` - Path including app/cycle/task/member
- ``{{ app.name }}``, ``{{ cycle.date }}``, ``{{ member.label }}``

Environment
-----------

The ``env`` specifies which environment configuration to use (defined in :file:`hosts.cfg`):

.. code-block:: ini

    [[content]]
    env = python_env

**No Environment:**

.. code-block:: ini

    env = None

**Multiple Environments:**

Use task inheritance:

.. code-block:: ini

    [base_python_task]
        [[content]]
        env = python_env

    [my_task]
    inherit = base_python_task
        [[content]]
        commandline = python my_script.py

Template
--------

The ``template`` specifies which Jinja2 template renders the job script (defaults to :file:`job.sh`):

.. code-block:: ini

    [[content]]
    template = custom_job.sh

Create custom templates in :file:`templates/` directory to override the default.

Task Inheritance
================

Tasks can inherit from other tasks to share configuration:

.. code-block:: ini

    [base_model_task]
        [[content]]
        env = ocean_env
        run_dir = {{ scratch_dir }}/{{ task_path }}

        [[submit]]
        queue = compute
        time = 04:00:00
        ncpus = 16

    [run_hindcast]
    inherit = base_model_task
        [[content]]
        commandline = ./ocean_model hindcast.nml

    [run_forecast]
    inherit = base_model_task
        [[content]]
        commandline = ./ocean_model forecast.nml

        [[submit]]
        time = 02:00:00  # Override with shorter time

**Inheritance Rules:**

- Child tasks override parent values
- Deeply nested sections are merged
- Set value to None to unset inherited value

Artifacts
=========

Artifacts are output files that woom tracks and validates.

Basic Artifacts
---------------

.. code-block:: ini

    [[artifacts]]
        [[[output_data]]]
        path = output.nc
        check = True

        [[[log_file]]]
        path = model.log
        check = True

**Fields:**

- ``path`` - File path (absolute or relative to run_dir)
- ``check`` - If True, woom verifies file exists after task completion
- ``callable`` - If True, path is a function name that generates the path

Multiple Files
--------------

Specify a list of files:

.. code-block:: ini

    [[artifacts]]
        [[[outputs]]]
        path = file1.nc, file2.nc, file3.nc
        check = True

Template Paths
--------------

Use template variables in paths:

.. code-block:: ini

    [[artifacts]]
        [[[model_output]]]
        path = {{ task_run_dir }}/output_{{ cycle.token }}.nc
        check = True

        [[[restart_file]]]
        path = {{ task_run_dir }}/restart_{{ cycle.end_date_str }}.nc
        check = True

Dynamic Paths with Callables
-----------------------------

For complex path generation, use a callable:

.. code-block:: ini

    [[artifacts]]
        [[[ensemble_outputs]]]
        path = generate_ensemble_paths
        check = True
        callable = True

            [[[[kwargs]]]]
            base_dir = {{ task_run_dir }}
            pattern = member_{:03d}.nc

Register the generator function in an extension file:

.. code-block:: python

    # ext/artifacts_generators.py
    from woom.tasks import ARTIFACTS_GENERATORS

    def generate_ensemble_paths(context, base_dir, pattern):
        """Generate paths for all ensemble members"""
        if context['member'] is None:
            return []
        paths = []
        for i in range(1, 51):  # 50 members
            paths.append(f"{base_dir}/{pattern.format(i)}")
        return paths

    ARTIFACTS_GENERATORS['generate_ensemble_paths'] = generate_ensemble_paths

Optional Artifacts
------------------

Set ``check = False`` for optional outputs:

.. code-block:: ini

    [[artifacts]]
        [[[required_output]]]
        path = results.nc
        check = True

        [[[optional_log]]]
        path = debug.log
        check = False

Template Filling
================

The ``[[fill]]`` section defines template files to fill before task execution.

Basic Template Filling
-----------------------

.. code-block:: ini

    [[fill]]
        [[[namelist]]]
        template = ocean.nml.j2
        destination = {{ task_run_dir }}/ocean.nml

        [[[config]]]
        template = config.xml.j2
        destination = {{ task_run_dir }}/config.xml

**How it Works:**

1. Template file is loaded from :file:`templates/` directory
2. Rendered with current context (task, cycle, member, params)
3. Written to destination path
4. Happens automatically before task command executes

Template Example
----------------

Create :file:`templates/ocean.nml.j2`:

.. code-block:: jinja

    &time_control
        start_date = "{{ cycle.begin_date_str }}"
        end_date = "{{ cycle.end_date_str }}"
        dt = {{ params.timestep }}
    /

    &grid
        nx = {{ params.grid_nx }}
        ny = {{ params.grid_ny }}
    /

    &output
        output_file = "{{ task_run_dir }}/output.nc"
        output_freq = {{ params.output_frequency }}
    /

Configure in tasks.cfg:

.. code-block:: ini

    [run_model]
        [[content]]
        commandline = ./ocean_model ocean.nml

        [[fill]]
            [[[namelist]]]
            template = ocean.nml.j2
            destination = {{ task_run_dir }}/ocean.nml

Multiple Templates
------------------

Fill multiple configuration files:

.. code-block:: ini

    [[fill]]
        [[[main_config]]]
        template = model.cfg.j2
        destination = {{ task_run_dir }}/model.cfg

        [[[forcing_list]]]
        template = forcings.txt.j2
        destination = {{ task_run_dir }}/forcings.txt

        [[[submission_script]]]
        template = post_process.sh.j2
        destination = {{ task_run_dir }}/post_process.sh

Member-Specific Configurations
-------------------------------

Generate different configurations for ensemble members:

.. code-block:: ini

    [[fill]]
        [[[member_config]]]
        template = ensemble_config.j2
        destination = {{ task_run_dir }}/config_{{ member.label }}.cfg

Template:

.. code-block:: jinja

    member_id = {{ member.id }}
    perturbation = {{ member.perturbation }}
    seed = {{ member.seed }}

Submission Configuration
========================

The ``[[submit]]`` section controls how and where tasks execute.

Queue Selection
---------------

.. code-block:: ini

    [[submit]]
    queue = normal

Queues are defined in :file:`hosts.cfg`. Common names:

- ``normal`` - Standard compute queue
- ``high_mem`` - High memory nodes
- ``gpu`` - GPU nodes
- ``debug`` - Fast debug queue with limits
- ``long`` - Extended time limit queue

Resource Requirements
---------------------

.. code-block:: ini

    [[submit]]
    nnodes = 2
    ncpus = 32
    ngpus = 4
    memory = 128GB
    pmem = 4GB
    time = 06:00:00

**Fields:**

- ``nnodes`` - Number of compute nodes
- ``ncpus`` - Number of CPU cores per task
- ``ngpus`` - Number of GPUs
- ``memory`` - Total memory limit
- ``pmem`` - Per-process memory limit
- ``time`` - Walltime limit (HH:MM:SS format)

**Scheduler Translation:**

Woom translates these to scheduler-specific options:

SLURM:
  - ``nnodes`` → ``--nodes=2``
  - ``ncpus`` → ``--ntasks-per-node=32``
  - ``time`` → ``--time=06:00:00``

PBS Pro:
  - ``nnodes=2, ncpus=32`` → ``-l select=2:ncpus=32``
  - ``time`` → ``-l walltime=06:00:00``

Task Blocking
-------------

.. code-block:: ini

    [[submit]]
    blocking = True

**blocking = True** (default):
  - Task must complete before dependent tasks start
  - Status is tracked
  - Failures stop the workflow

**blocking = False**:
  - Task runs but doesn't block dependents
  - Used for monitoring, logging, non-critical tasks
  - Gracefully terminated when workflow completes

Example: Monitoring Task
-------------------------

.. code-block:: ini

    [monitor_progress]
        [[content]]
        commandline = watch -n 60 'ls -lh {{ task_run_dir }}/output*'
        run_dir = {{ workflow_dir }}

        [[submit]]
        blocking = False  # Don't wait for this
        queue = debug

Email Notifications
-------------------

.. code-block:: ini

    [[submit]]
    mail = user@example.com

Sends email on task completion/failure (if scheduler supports it).

Complete Task Examples
======================

Example 1: Simple Python Script
--------------------------------

.. code-block:: ini

    [analyze_data]
        [[content]]
        commandline = python analyze.py {{ cycle.date }}
        run_dir = {{ workflow_dir }}/analysis
        env = python_data

        [[artifacts]]
            [[[results]]]
            path = results_{{ cycle.token }}.csv
            check = True

        [[submit]]
        queue = normal
        ncpus = 1
        memory = 8GB
        time = 00:30:00

Example 2: MPI Simulation
--------------------------

.. code-block:: ini

    [run_ocean_model]
        [[content]]
        commandline = mpirun -n {{ ncpus }} ./ocean_model ocean.nml
        run_dir = {{ scratch_dir }}/{{ task_path }}
        env = ocean_env
        template = mpi_job.sh

        [[fill]]
            [[[namelist]]]
            template = ocean.nml.j2
            destination = {{ task_run_dir }}/ocean.nml

        [[artifacts]]
            [[[output]]]
            path = output_{{ cycle.end_date_str }}.nc
            check = True

            [[[restart]]]
            path = restart_{{ cycle.end_date_str }}.nc
            check = True

        [[submit]]
        queue = compute
        nnodes = 4
        ncpus = 128
        time = 08:00:00
        memory = 256GB

Example 3: Data Download
-------------------------

.. code-block:: ini

    [download_forcing]
        [[content]]
        commandline = '''
            wget https://data.example.com/forcing_{{ cycle.date }}.nc
            mv forcing_{{ cycle.date }}.nc {{ task_run_dir }}/
            '''
        run_dir = {{ params.forcing_dir }}

        [[artifacts]]
            [[[forcing_file]]]
            path = {{ params.forcing_dir }}/forcing_{{ cycle.date }}.nc
            check = True

        [[submit]]
        queue = debug
        ncpus = 1
        time = 00:15:00

Example 4: Post-Processing with Ensemble
-----------------------------------------

.. code-block:: ini

    [compute_ensemble_mean]
        [[content]]
        commandline = python ensemble_mean.py --input {{ task_run_dir }} --output mean.nc
        run_dir = {{ scratch_dir }}/postprocess
        env = python_analysis

        [[fill]]
            [[[file_list]]]
            template = ensemble_files.txt.j2
            destination = {{ task_run_dir }}/files.txt

        [[artifacts]]
            [[[mean_output]]]
            path = mean_{{ cycle.token }}.nc
            check = True

            [[[std_output]]]
            path = std_{{ cycle.token }}.nc
            check = True

        [[submit]]
        queue = normal
        ncpus = 8
        memory = 64GB
        time = 01:00:00

Example 5: Conditional Execution
---------------------------------

.. code-block:: ini

    [conditional_analysis]
        [[content]]
        commandline = '''
            if [ -f {{ task_run_dir }}/trigger.flag ]; then
                python special_analysis.py
            else
                echo "Skipping - no trigger file"
            fi
            '''
        run_dir = {{ workflow_dir }}/analysis

        [[submit]]
        queue = debug
        ncpus = 1
        time = 00:10:00

Skipping Tasks
==============

A task can be excluded from submission while remaining in the task tree.
This is useful when a task has already produced its artifacts in a previous
run and you want downstream tasks to reference those artifacts without
re-running the task itself.

A skipped task:

- is **never submitted** to the scheduler
- is **never cleaned** (its submission directory and artifacts are preserved)
- contributes **no scheduler dependencies** to downstream tasks (they run immediately)
- still appears in ``woom show status`` with status ``SKIPPED``
- still appears in ``woom show artifacts`` with its artifact paths

Static Skip (in :file:`tasks.cfg`)
------------------------------------

Set ``skip = True`` directly on a task to permanently exclude it from submission
within a given configuration:

.. code-block:: ini

    [preprocess]
    skip = True
        [[content]]
        commandline = python preprocess.py
        [[artifacts]]
            [[[output]]]
            path = {{ task_run_dir }}/preprocessed.nc
            check = True

    [run_model]
        [[content]]
        commandline = ./model preprocessed.nc
        # can still read preprocess artifacts even though it was skipped

Combined with :ref:`task inheritance <indepth.tasks>`, this lets you activate
or deactivate tasks without restructuring the workflow:

.. code-block:: ini

    [base_preprocess]
        [[content]]
        commandline = python preprocess.py
        [[artifacts]]
            [[[output]]]
            path = {{ task_run_dir }}/preprocessed.nc

    [preprocess]
    inherit = base_preprocess
    skip = True   # disable for this experiment

.. warning::
   When a task is skipped its artifacts must already exist on disk.  If they
   do not, downstream tasks that depend on those files will fail at runtime.

See Also
--------

- :ref:`indepth.workflow` — skip tasks at runtime without editing :file:`tasks.cfg`

Task Organization Strategies
=============================

By Purpose
----------

.. code-block:: ini

    # Setup tasks
    [create_workspace]
    [download_inputs]
    [compile_code]

    # Core computation
    [run_model]
    [run_diagnostics]

    # Post-processing
    [extract_variables]
    [compute_statistics]
    [create_plots]

    # Finalization
    [merge_outputs]
    [cleanup]

By Inheritance Hierarchy
-------------------------

.. code-block:: ini

    [base_task]
        [[submit]]
        queue = normal
        time = 01:00:00

    [base_python_task]
    inherit = base_task
        [[content]]
        env = python_env

    [base_model_task]
    inherit = base_task
        [[content]]
        env = model_env
        run_dir = {{ scratch_dir }}/{{ task_path }}

    [specific_task]
    inherit = base_python_task
        [[content]]
        commandline = python specific.py

Best Practices
==============

1. **Use Inheritance**: Define common configurations once in base tasks

2. **Validate Artifacts**: Set ``check = True`` for critical outputs

3. **Template Configurations**: Use ``[[fill]]`` instead of hardcoding parameters

4. **Request Appropriate Resources**: Don't over-request resources, it delays scheduling

5. **Use Meaningful Names**: Task names should describe what they do

6. **Set Reasonable Timeouts**: Add buffer but avoid excessive walltime requests

7. **Test Locally**: Use a simple host configuration to test tasks before HPC submission

8. **Document Complex Commands**: Add comments explaining non-obvious command sequences

9. **Handle Errors**: Consider exit codes and error handling in complex command sequences

10. **Organize by Stage**: Name tasks to indicate which workflow stage they belong to

Common Pitfalls
===============

1. **Forgetting run_dir**: Relative artifact paths need run_dir defined

2. **Missing Environment**: Tasks fail if environment doesn't exist on host

3. **Incorrect Resource Requests**: nnodes vs ncpus confusion varies by scheduler

4. **Template Syntax Errors**: Test templates independently before workflow run

5. **Artifact Path Mismatches**: Ensure artifact paths match actual output locations

6. **Blocking Loops**: Non-blocking tasks should not create dependencies

7. **Over-requesting Resources**: Excessive requests delay scheduling

Troubleshooting
===============

Task Won't Submit
-----------------

Check:

- Queue exists in host configuration
- Resource requests are valid for queue
- Environment is available on host
- Commandline syntax is correct

Task Fails Immediately
----------------------

Check:

- run_dir exists or can be created
- Command is executable
- Environment loads correctly
- Input files exist

Artifacts Not Found
-------------------

Check:

- Artifact path is correct (absolute or relative to run_dir)
- Task actually produces the file
- Template variables render correctly
- File permissions allow access

See Also
========

- :ref:`indepth.workflow` - Organize tasks into workflows
- :ref:`indepth.artifacts` - Detailed artifact handling
- :ref:`indepth.templating` - Template filling system
- :ref:`cfgspecs.tasks` - Complete configuration reference
