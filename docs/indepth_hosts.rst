.. _indepth.hosts:

Host Configuration In-Depth
############################

The :file:`hosts.cfg` file defines execution environments where your tasks run. This includes local machines, HPC clusters, cloud resources, and how to access software environments on each.

Structure Overview
==================

A host configuration defines:

.. code-block:: ini

    [hostname]
    scheduler = slurm
    scratch_dir = /scratch/$USER

        [[patterns]]
        # Auto-detect this host

        [[queues]]
            [[[normal]]]
            # Queue definitions

        [[envs]]
            [[[myenv]]]
            # Environment configurations

Host Basics
===========

Minimal Host
------------

.. code-block:: ini

    [local]
    scheduler = background
    scratch_dir = /tmp

**Required Fields:**

- Host section name (e.g., ``[local]``, ``[hpc_cluster]``)
- ``scheduler`` - How to run jobs (background, slurm, pbspro)
- ``scratch_dir`` - Temporary working directory

Host Selection
--------------

**Explicit Selection:**

.. code-block:: bash

    woom run --host hpc_cluster

**Auto-Detection:**

Configure patterns to auto-detect:

.. code-block:: ini

    [datarmor]
    scheduler = slurm

        [[patterns]]
        hostname = datarmor.*

When hostname matches pattern, this host is automatically used.

Multiple Hosts
--------------

Define different configurations for different systems:

.. code-block:: ini

    [laptop]
    scheduler = background
    scratch_dir = /tmp

    [workstation]
    scheduler = background
    scratch_dir = /scratch

    [university_cluster]
    scheduler = slurm
    scratch_dir = /scratch/$USER

    [national_supercomputer]
    scheduler = pbspro
    scratch_dir = /work/$USER

Schedulers
==========

Background Scheduler
--------------------

For local execution without a batch scheduler:

.. code-block:: ini

    [local]
    scheduler = background
    scratch_dir = /tmp

**Characteristics:**

- Jobs run as background processes
- No queuing system
- Immediate execution
- Good for development and testing
- Limited parallelism

SLURM Scheduler
---------------

For systems using SLURM Workload Manager:

.. code-block:: ini

    [slurm_cluster]
    scheduler = slurm
    scratch_dir = /scratch/$USER

        [[queues]]
            [[[normal]]]
            partition = compute
            qos = normal
            account = myproject

            [[[gpu]]]
            partition = gpu
            gres = gpu:4
            account = myproject

**SLURM-Specific Options:**

- ``partition`` - SLURM partition name
- ``qos`` - Quality of service
- ``account`` - Billing account
- ``reservation`` - Reservation name
- ``gres`` - Generic resources (like GPUs)
- ``constraint`` - Node constraints

PBS Pro Scheduler
-----------------

For systems using PBS Professional:

.. code-block:: ini

    [pbspro_cluster]
    scheduler = pbspro
    scratch_dir = /work/$USER

        [[queues]]
            [[[normal]]]
            queue_name = workq
            project = PROJ001

**PBS-Specific Options:**

- ``queue_name`` - PBS queue name
- ``project`` - Project code for accounting

Queue Configuration
===================

Queues define resource pools and policies.

Basic Queue
-----------

.. code-block:: ini

    [[queues]]
        [[[normal]]]
        partition = compute
        account = myaccount

Generic vs Scheduler-Specific
------------------------------

**Generic Options** (work across schedulers):

.. code-block:: ini

    [[[normal]]]
    # Translated automatically to scheduler syntax

**Scheduler-Specific Options** (SLURM example):

.. code-block:: ini

    [[[gpu_queue]]]
    partition = gpu
    gres = gpu:4
    qos = high

Multiple Queues
----------------

Define different queues for different resource needs:

.. code-block:: ini

    [[queues]]
        [[[debug]]]
        partition = debug
        # Fast queue with limited time

        [[[normal]]]
        partition = compute
        account = project123

        [[[highmem]]]
        partition = highmem
        account = project123

        [[[gpu]]]
        partition = gpu
        gres = gpu:4
        account = project123

        [[[long]]]
        partition = compute
        qos = long
        account = project123

Queue Inheritance
-----------------

Avoid repetition with inheritance:

.. code-block:: ini

    [[queues]]
        [[[base]]]
        account = myproject
        partition = compute

        [[[normal]]]
        inherit = base

        [[[highmem]]]
        inherit = base
        partition = highmem

        [[[gpu]]]
        inherit = base
        partition = gpu
        gres = gpu:4

Environment Configuration
=========================

Environments define software stacks for tasks.

No Environment
--------------

Tasks can run without special environment:

.. code-block:: ini

    # No [[envs]] section needed
    # Tasks with env = None use default environment

Module Environment
------------------

Load software via environment modules:

.. code-block:: ini

    [[envs]]
        [[[ocean_model]]]
        modules = netcdf/4.8.1, hdf5/1.12.0, openmpi/4.1.1

        [[[python_env]]]
        modules = python/3.9, scipy-stack/2023a

**Multiple Modules:**

Comma-separated list loads in order.

Conda/Mamba Environment
-----------------------

Activate conda environments:

.. code-block:: ini

    [[envs]]
        [[[analysis]]]
        conda = analysis_env

        [[[forecast]]]
        mamba = forecast_env

Virtualenv/venv
---------------

Activate Python virtual environments:

.. code-block:: ini

    [[envs]]
        [[[python_analysis]]]
        venv = /home/user/envs/analysis

UV Virtual Environment
----------------------

Activate UV environments:

.. code-block:: ini

    [[envs]]
        [[[fast_python]]]
        uv_venv = /home/user/.venv

Combined Environments
---------------------

Combine multiple environment types:

.. code-block:: ini

    [[envs]]
        [[[full_stack]]]
        modules = gcc/11.2, openmpi/4.1
        conda = scientific_py
        exports = OMP_NUM_THREADS=8, MKL_NUM_THREADS=8

Raw Text Environments
---------------------

For complex setup, provide raw shell commands:

.. code-block:: ini

    [[envs]]
        [[[custom]]]
        raw_text = '''
            source /opt/custom/setup.sh
            export CUSTOM_VAR=value
            module load special_software
            '''

Environment Variables
---------------------

Set variables in the environment:

.. code-block:: ini

    [[envs]]
        [[[model_env]]]
        modules = netcdf/4.8
        exports = '''
            OMP_NUM_THREADS=16
            DATA_ROOT=/data/ocean
            MODEL_VERSION=v2.3
            '''

Auto-Detection Patterns
=======================

Configure pattern matching to automatically select hosts.

Hostname Pattern
----------------

.. code-block:: ini

    [university_hpc]
    scheduler = slurm

        [[patterns]]
        hostname = login[0-9]+.hpc.university.edu

Matches: login1.hpc.university.edu, login2.hpc.university.edu, etc.

Environment Variables
---------------------

.. code-block:: ini

    [national_center]
    scheduler = pbspro

        [[patterns]]
        env_vars = CLUSTER_NAME=national_hpc

Matches when environment variable is set.

Multiple Patterns
-----------------

Combine patterns (AND logic):

.. code-block:: ini

    [specific_cluster]
    scheduler = slurm

        [[patterns]]
        hostname = compute.*
        env_vars = SITE=facility_a

Complete Host Examples
======================

Example 1: Development Laptop
------------------------------

.. code-block:: ini

    [laptop]
    scheduler = background
    scratch_dir = /tmp/woom

        [[envs]]
            [[[python]]]
            conda = dev_env

Example 2: University SLURM Cluster
------------------------------------

.. code-block:: ini

    [university_hpc]
    scheduler = slurm
    scratch_dir = /scratch/$USER

        [[patterns]]
        hostname = login.*.hpc.edu

        [[queues]]
            [[[debug]]]
            partition = debug
            account = course101
            # 30 min limit

            [[[normal]]]
            partition = compute
            account = research_proj
            qos = normal

            [[[highmem]]]
            partition = highmem
            account = research_proj

        [[envs]]
            [[[ocean_model]]]
            modules = gcc/11, netcdf-fortran/4.5, openmpi/4.1

            [[[python_analysis]]]
            modules = python/3.10, scipy/1.9
            exports = PYTHONUNBUFFERED=1

Example 3: National Supercomputer (PBS)
----------------------------------------

.. code-block:: ini

    [national_hpc]
    scheduler = pbspro
    scratch_dir = /work/$USER/scratch

        [[patterns]]
        hostname = login.*.national.gov

        [[queues]]
            [[[standard]]]
            queue_name = standard
            project = ATMO12345

            [[[large]]]
            queue_name = capability
            project = ATMO12345

        [[envs]]
            [[[intel_mpi]]]
            raw_text = '''
                module purge
                module load intel/2023
                module load impi/2021
                module load netcdf/4.9
                '''

            [[[analysis]]]
            modules = python/3.11
            venv = /work/$USER/venvs/analysis

Example 4: Multi-Site Configuration
------------------------------------

.. code-block:: ini

    # Site A - SLURM
    [site_a]
    scheduler = slurm
    scratch_dir = /scratch/$USER

        [[patterns]]
        hostname = login-a.*

        [[queues]]
            [[[normal]]]
            partition = compute
            account = proj_a

        [[envs]]
            [[[model]]]
            modules = netcdf/4.8, openmpi/4.1

    # Site B - PBS
    [site_b]
    scheduler = pbspro
    scratch_dir = /work/$USER

        [[patterns]]
        hostname = login-b.*

        [[queues]]
            [[[normal]]]
            queue_name = standard
            project = proj_b

        [[envs]]
            [[[model]]]
            modules = netcdf/4.7, mpt/2.25

    # Local development
    [local]
    scheduler = background
    scratch_dir = /tmp

        [[envs]]
            [[[model]]]
            conda = ocean_dev

Advanced Features
=================

Host Inheritance
----------------

Share configuration between similar hosts:

.. code-block:: ini

    [base_slurm]
    scheduler = slurm

        [[queues]]
            [[[normal]]]
            account = myproject

    [cluster_a]
    inherit = base_slurm
    scratch_dir = /scratch/a/$USER

        [[patterns]]
        hostname = login-a.*

    [cluster_b]
    inherit = base_slurm
    scratch_dir = /scratch/b/$USER

        [[patterns]]
        hostname = login-b.*

Custom Scheduler Options
-------------------------

Pass extra options to scheduler:

.. code-block:: ini

    [[queues]]
        [[[special]]]
        partition = compute
        # Custom SLURM options
        extra_options = --constraint=ib&haswell

Parameter Overrides
-------------------

Override workflow parameters per host:

.. code-block:: ini

    [laptop]
    scratch_dir = /tmp

        [[params]]
        nprocs = 4  # Laptop has fewer cores

    [supercomputer]
    scratch_dir = /scratch/$USER

        [[params]]
        nprocs = 1024  # Use many cores

Access from templates:

.. code-block:: jinja

    mpirun -n {{ params.nprocs }} ./model

Best Practices
==============

1. **Use Auto-Detection**: Configure patterns for automatic host selection

2. **Organize by Purpose**: Group queues logically (debug, normal, long, highmem, gpu)

3. **Document Requirements**: Comment what modules/software are needed

4. **Test Locally First**: Have a local/background host for testing

5. **Use Inheritance**: Avoid repeating common configurations

6. **Keep Secrets Out**: Don't put passwords or keys in configuration

7. **Environment Modules**: Prefer modules over hardcoded paths

8. **Validate Accounts**: Ensure account/project codes are correct

9. **Check Queue Limits**: Know walltime and resource limits

10. **Version Control**: Track host configs in git (without secrets)

Common Patterns
===============

Pattern 1: Development + Production
------------------------------------

.. code-block:: ini

    [dev]
    scheduler = background
    scratch_dir = /tmp

    [prod]
    scheduler = slurm
    scratch_dir = /scratch/$USER
        [[queues]]
            [[[normal]]]
            partition = compute

Pattern 2: Multi-Tier Queues
-----------------------------

.. code-block:: ini

    [[queues]]
        [[[debug]]]
        # Fast, limited
        partition = debug

        [[[normal]]]
        # Standard
        partition = compute

        [[[long]]]
        # Extended time
        partition = compute
        qos = long

        [[[highmem]]]
        # More memory
        partition = highmem

        [[[gpu]]]
        # GPU access
        partition = gpu
        gres = gpu:4

Pattern 3: Software Stacks
---------------------------

.. code-block:: ini

    [[envs]]
        [[[gnu_stack]]]
        modules = gcc/11, openmpi/4, netcdf/4.8

        [[[intel_stack]]]
        modules = intel/2023, impi/2021, netcdf/4.9

        [[[python_stack]]]
        modules = python/3.10
        conda = analysis_env

Troubleshooting
===============

Auto-Detection Not Working
---------------------------

Check:

- Pattern matches actual hostname: ``echo $HOSTNAME``
- Environment variables are set: ``env | grep CLUSTER``
- No typos in pattern syntax
- Multiple hosts don't match (creates ambiguity)

Module Load Fails
------------------

Check:

- Module exists: ``module avail modulename``
- Module dependencies loaded first
- Correct module version specified
- Module system initialized

Environment Not Activated
--------------------------

Check:

- Conda/venv path is correct
- Environment exists: ``conda env list``
- Correct environment type specified (conda vs mamba vs venv vs uv_venv)
- Shell initialization allows activation

Jobs Not Submitting
--------------------

Check:

- Queue/partition exists: ``sinfo`` (SLURM) or ``qstat -q`` (PBS)
- Account/project is valid
- Resource requests within limits
- User has access to queue

Migration Guide
===============

Moving Between Systems
-----------------------

When moving workflow to new system:

1. Create new host configuration
2. Test with simple task: ``woom run --host newhpc``
3. Adjust paths (scratch_dir, data locations)
4. Update queue/partition names
5. Verify environment modules/software
6. Test full workflow

Scheduler Migration
-------------------

Moving from SLURM to PBS (or vice versa):

1. Change ``scheduler`` setting
2. Update queue configuration (partition → queue_name, etc.)
3. Test submission with simple job
4. Adjust resource request translations if needed
5. Update any scheduler-specific custom options

See Also
========

- :ref:`indepth.tasks` - Configure tasks to run on hosts
- :ref:`cfgspecs.hosts` - Complete configuration reference
- :ref:`inputs_envvars` - Environment variables available
