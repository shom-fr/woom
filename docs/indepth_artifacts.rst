.. _indepth.artifacts:

Artifacts In-Depth
##################

Artifacts are output files that your tasks produce. Woom tracks artifacts to verify task completion, provide visibility into outputs, and help with workflow debugging and data management.

What Are Artifacts?
===================

Artifacts represent important output files that:

- Indicate successful task completion
- Serve as inputs to downstream tasks
- Represent final products of your workflow
- Need to be validated and tracked

**Examples:**

- Model output files (NetCDF, HDF5)
- Restart/checkpoint files
- Analysis results (CSV, plots)
- Log files
- Processed data products

Why Track Artifacts?
====================

1. **Validation**: Verify tasks completed successfully by checking outputs exist
2. **Debugging**: Quickly identify which files are missing
3. **Documentation**: See what files your workflow produces
4. **Dependencies**: Understand data flow between tasks
5. **Data Management**: Identify files to archive or clean up

Basic Configuration
===================

Simple Artifact
---------------

.. code-block:: ini

    [run_model]
        [[artifacts]]
            [[[output]]]
            path = output.nc
            check = True

**Fields:**

- ``path``: File path (absolute or relative to ``run_dir``)
- ``check``: Whether to verify file exists after task completes (default: True)
- ``callable``: Whether path is a function name (default: False)

Multiple Artifacts
------------------

.. code-block:: ini

    [run_model]
        [[artifacts]]
            [[[output]]]
            path = output.nc
            check = True

            [[[restart]]]
            path = restart.nc
            check = True

            [[[log]]]
            path = model.log
            check = False  # Optional file

Absolute vs Relative Paths
===========================

Relative Paths
--------------

Relative to task's ``run_dir``:

.. code-block:: ini

    [task]
        [[content]]
        run_dir = /scratch/run

        [[artifacts]]
            [[[output]]]
            path = results/output.nc  # → /scratch/run/results/output.nc

Absolute Paths
--------------

.. code-block:: ini

    [[artifacts]]
        [[[shared_output]]]
        path = /data/shared/results.nc  # Absolute path

Template Paths
--------------

Use template variables:

.. code-block:: ini

    [[artifacts]]
        [[[output]]]
        path = {{ run_dir }}/output_{{ cycle.token }}.nc

        [[[dated_output]]]
        path = /data/outputs/{{ cycle.date.year }}/{{ cycle.date.month }}/data.nc

Artifact Checking
=================

Mandatory Artifacts
-------------------

.. code-block:: ini

    [[artifacts]]
        [[[critical_output]]]
        path = output.nc
        check = True  # Task fails if missing

If ``check = True`` and file doesn't exist after task completes, woom marks the task as failed.

Optional Artifacts
------------------

.. code-block:: ini

    [[artifacts]]
        [[[debug_log]]]
        path = debug.log
        check = False  # Warning only if missing

If ``check = False``, missing files generate warnings but don't fail the task.

Multiple Files
==============

Lists of Files
--------------

.. code-block:: ini

    [[artifacts]]
        [[[outputs]]]
        path = output1.nc, output2.nc, output3.nc
        check = True

Woom checks each file in the list.

Wildcards (Not Supported)
--------------------------

Woom doesn't support glob patterns in paths. Use callable generators instead:

.. code-block:: ini

    [[artifacts]]
        [[[all_outputs]]]
        path = generate_output_list
        callable = True
        check = True

Dynamic Artifact Paths
======================

Using Templates
---------------

Cycle-Dependent:

.. code-block:: ini

    [[artifacts]]
        [[[daily_output]]]
        path = {{ run_dir }}/output_{{ cycle.date_str }}.nc

Member-Dependent:

.. code-block:: ini

    [[artifacts]]
        [[[ensemble_output]]]
        path = {{ run_dir }}/output_{{ member.label }}.nc

Combined:

.. code-block:: ini

    [[artifacts]]
        [[[result]]]
        path = {{ scratch_dir }}/{{ task_path }}/result_{{ cycle.token }}_{{ member.label }}.nc

Using Callable Generators
--------------------------

For complex path generation:

**tasks.cfg:**

.. code-block:: ini

    [[artifacts]]
        [[[ensemble_outputs]]]
        path = generate_ensemble_outputs
        callable = True
        check = True

            [[[[kwargs]]]]
            base_dir = {{ run_dir }}
            prefix = member

**ext/artifacts_generators.py:**

.. code-block:: python

    from woom.tasks import ARTIFACTS_GENERATORS

    def generate_ensemble_outputs(context, base_dir, prefix):
        """Generate list of ensemble output files"""
        outputs = []
        if context.get('member'):
            # Single member - return its file
            member = context['member']
            outputs.append(f"{base_dir}/{prefix}_{member.label}.nc")
        else:
            # No member context - return all expected files
            workflow = context['workflow']
            for member in workflow.members:
                outputs.append(f"{base_dir}/{prefix}_{member.label}.nc")
        return outputs

    ARTIFACTS_GENERATORS['generate_ensemble_outputs'] = generate_ensemble_outputs

Advanced: Time Series
---------------------

Generate daily file list:

.. code-block:: python

    def generate_daily_outputs(context, output_dir, pattern):
        """Generate daily output files for cycle"""
        outputs = []
        cycle = context.get('cycle')
        if not cycle:
            return outputs

        current_date = cycle.begin_date
        while current_date < cycle.end_date:
            filename = pattern.format(
                year=current_date.year,
                month=current_date.month,
                day=current_date.day
            )
            outputs.append(f"{output_dir}/{filename}")
            current_date += pd.Timedelta(days=1)

        return outputs

    ARTIFACTS_GENERATORS['daily_outputs'] = generate_daily_outputs

**Usage:**

.. code-block:: ini

    [[artifacts]]
        [[[daily_files]]]
        path = daily_outputs
        callable = True

            [[[[kwargs]]]]
            output_dir = {{ run_dir }}/daily
            pattern = output_{year:04d}{month:02d}{day:02d}.nc

Viewing Artifacts
=================

Command Line
------------

List all artifacts:

.. code-block:: bash

    woom show artifacts

Filter by task:

.. code-block:: bash

    woom show artifacts --task-name run_model

Filter by cycle:

.. code-block:: bash

    woom show artifacts --cycle 2020-01-01

From Python
-----------

.. code-block:: python

    # Get all artifacts for a task
    artifacts = workflow.get_task_artifacts('run_model', cycle='2020-01-01')

    for name, paths in artifacts.items():
        print(f"{name}: {paths}")

    # Get specific artifact
    output_path = workflow.get_task_artifact_paths(
        'output',
        'run_model',
        cycle='2020-01-01'
    )

    # Check if exists
    import os
    if os.path.exists(output_path):
        print("Output file exists")

Artifacts DataFrame
-------------------

.. code-block:: python

    # Get DataFrame of all artifacts
    df = workflow.get_artifacts()
    print(df)

    # Filter
    df_model = workflow.get_artifacts(task_name='run_model')

    # Check existence
    missing = df[~df['EXISTS?']]
    print(f"Missing files: {len(missing)}")

Common Patterns
===============

Model Outputs
-------------

.. code-block:: ini

    [run_ocean_model]
        [[artifacts]]
            [[[output]]]
            path = {{ run_dir }}/ocean_{{ cycle.token }}.nc
            check = True

            [[[restart]]]
            path = {{ run_dir }}/restart_{{ cycle.end_date_str }}.nc
            check = True

            [[[diagnostics]]]
            path = {{ run_dir }}/diagnostics.nc
            check = False  # Optional

Analysis Results
----------------

.. code-block:: ini

    [compute_statistics]
        [[artifacts]]
            [[[stats]]]
            path = {{ scratch_dir }}/analysis/stats_{{ cycle.token }}.csv
            check = True

            [[[plots]]]
            path = plot_sst.png, plot_currents.png, plot_salinity.png
            check = False  # Plots are optional

Post-Processing
---------------

.. code-block:: ini

    [merge_outputs]
        [[artifacts]]
            [[[merged_file]]]
            path = /data/final/merged_{{ app.exp }}_{{ cycle.date.year }}.nc
            check = True

Data Download
-------------

.. code-block:: ini

    [download_forcing]
        [[artifacts]]
            [[[forcing_file]]]
            path = {{ params.forcing_dir }}/era5_{{ cycle.date_str }}.nc
            check = True

Ensemble Processing
-------------------

.. code-block:: ini

    [ensemble_mean]
        [[artifacts]]
            [[[mean]]]
            path = {{ scratch_dir }}/ensemble/mean_{{ cycle.token }}.nc
            check = True

            [[[std]]]
            path = {{ scratch_dir }}/ensemble/std_{{ cycle.token }}.nc
            check = True

            [[[individual_members]]]
            path = list_ensemble_files
            callable = True
            check = False  # Don't fail if individual files missing

Artifact Best Practices
========================

1. **Track Important Outputs**: Define artifacts for critical files only

2. **Use Meaningful Names**: Artifact names should describe what the file contains

3. **Set Appropriate Check Flags**:
   - ``check=True`` for required outputs
   - ``check=False`` for optional/debug files

4. **Use Template Variables**: Make paths dynamic with cycle/member information

5. **Organize Output Directories**: Use consistent directory structures

6. **Document Expectations**: Comment what each artifact represents

7. **Validate Paths**: Test that paths are correct before running workflow

8. **Handle Missing Gracefully**: Use ``check=False`` for truly optional outputs

9. **Consider Downstream**: Think about which files downstream tasks need

10. **Archive Strategy**: Identify which artifacts to keep long-term

Troubleshooting
===============

Artifact Not Found
------------------

**Symptoms**: Task marked as failed, "Artifact not found" message

**Causes:**

1. Task didn't create the file
2. Wrong path in configuration
3. File created in different location
4. Permissions prevent access

**Debug:**

.. code-block:: bash

    # Check what files task created
    ls -R /path/to/run/dir

    # Compare to expected artifact path
    woom show artifacts --task-name my_task

    # Check job output
    cat jobs/*/my_task/job.out

**Solutions:**

- Verify task command actually creates file
- Check path template rendering
- Ensure run_dir is set correctly
- Use absolute paths if needed
- Check file permissions

Path Template Errors
--------------------

**Symptoms**: Path doesn't render correctly

**Causes:**

- Syntax error in template
- Variable undefined in context
- Wrong variable used

**Debug:**

.. code-block:: python

    # Check rendered path
    context = workflow.get_context(task_name='my_task', cycle='2020-01-01')
    task = workflow.get_task('my_task')
    task.set_context(context)
    artifacts = task.render_artifacts()
    print(artifacts)

**Solutions:**

- Test template syntax
- Verify variables exist in context
- Use ``| default()`` for optional variables
- Check for typos in variable names

Callable Not Working
--------------------

**Symptoms**: Artifact generator doesn't run or errors

**Causes:**

- Function not registered
- Wrong function signature
- Runtime error in function

**Debug:**

.. code-block:: python

    # Check if registered
    from woom.tasks import ARTIFACTS_GENERATORS
    print('my_generator' in ARTIFACTS_GENERATORS)

    # Test function directly
    result = ARTIFACTS_GENERATORS['my_generator'](context, **kwargs)
    print(result)

**Solutions:**

- Ensure function is registered in ARTIFACTS_GENERATORS
- Check function signature matches: ``func(context, **kwargs)``
- Add error handling in generator function
- Test with simple case first

Wrong Files Checked
-------------------

**Symptoms**: Task succeeds but didn't create expected files

**Causes:**

- ``check=False`` on critical artifacts
- Wrong artifact configured
- Files created with different names

**Solutions:**

- Set ``check=True`` for required outputs
- Verify artifact names match actual outputs
- Use callable to list actual files created
- Review task output logs

Performance Issues
------------------

**Symptoms**: Artifact checking is slow

**Causes:**

- Too many artifacts defined
- Network file system latency
- Large file lists from callables

**Solutions:**

- Only track essential artifacts
- Use ``check=False`` for non-critical files
- Optimize callable generators
- Consider aggregate artifacts (one check for directory)

Integration with Workflow
==========================

Artifacts as Dependencies
-------------------------

While woom doesn't automatically create task dependencies based on artifacts, you can design your workflow to reflect these relationships:

.. code-block:: ini

    # Stage 1: Create data
    [[prolog]]
    prepare = download_forcing

    # Stage 2: Use data
    [[cycles]]
    simulate = run_model  # Uses forcing from prepare

Checking Before Run
-------------------

.. code-block:: python

    # Verify previous task's artifacts before running
    artifacts = workflow.get_task_artifacts('previous_task', cycle='2020-01-01')

    for name, paths in artifacts.items():
        for path in paths:
            if not os.path.exists(path):
                raise RuntimeError(f"Required input missing: {path}")

    # Now safe to run dependent task
    workflow.run()

Cleanup Strategy
----------------

.. code-block:: bash

    # Show all artifacts with existence status
    woom show artifacts > artifact_inventory.txt

    # Use artifact info for cleanup decisions
    # Keep final products, remove intermediate files

See Also
========

- :ref:`indepth.tasks` - Task configuration including artifacts
- :ref:`cfgspecs.tasks` - Artifact configuration reference
- :ref:`woom_show_artifacts` - Command line artifact viewing
