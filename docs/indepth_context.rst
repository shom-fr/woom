.. _indepth.context:

Context and Variables In-Depth
###############################

The context is a dictionary of variables available when rendering templates and configuration files. Understanding what variables are available and how to use them is key to creating flexible, reusable workflows.

What is the Context?
=====================

The context provides information about:

- The workflow (application name, paths)
- The current task (name, run directory)
- The current cycle (dates, tokens)
- The current ensemble member (ID, parameters)
- Custom parameters from configuration
- Environment variables

Access in Templates
-------------------

Variables are accessed using Jinja2 syntax:

.. code-block:: jinja

    Task: {{ task.name }}
    Cycle: {{ cycle.begin_date }} to {{ cycle.end_date }}
    Run directory: {{ task_run_dir }}
    Parameter value: {{ params.my_param }}

Access in Python
----------------

From Python code:

.. code-block:: python

    context = workflow.get_context(task_name='run_model', cycle='2020-01-01')
    print(context['task'].name)
    print(context['cycle'].begin_date)
    print(context['params']['my_param'])

Workflow Variables
==================

Application Information
-----------------------

.. code-block:: jinja

    {{ app.name }}    # Application name
    {{ app.conf }}    # Configuration name
    {{ app.exp }}     # Experiment name

**Example:**

Configuration:

.. code-block:: ini

    [app]
    name = ocean_model
    conf = north_atlantic
    exp = test_2020

Template:

.. code-block:: jinja

    Application: {{ app.name }}
    Configuration: {{ app.conf }}
    Experiment: {{ app.exp }}

Result:

.. code-block:: text

    Application: ocean_model
    Configuration: north_atlantic
    Experiment: test_2020

Workflow Paths
--------------

.. code-block:: jinja

    {{ workflow_dir }}      # Directory containing workflow.cfg
    {{ task_submission_dir }}    # Where job scripts are created
    {{ script_path }}       # Path to current job script

**Example:**

.. code-block:: jinja

    Workflow location: {{ workflow_dir }}
    Job script: {{ script_path }}

Task Variables
==============

Task Information
----------------

.. code-block:: jinja

    {{ task.name }}       # Task name
    {{ task_name }}       # Also available (shorthand)
    {{ task_path }}       # Full path including app/cycle/task/member
    {{ task_run_dir }}         # Where task executes

**Example:**

.. code-block:: jinja

    Executing task: {{ task.name }}
    Task path: {{ task_path }}
    Working directory: {{ task_run_dir }}

**task_path Components:**

For workflow ``ocean_model/config1/exp1``, task ``run_model``, cycle ``2020-01-01``, member ``member001``:

.. code-block:: text

    task_path = ocean_model/config1/exp1/2020-01-01/run_model/member001

Task Object
-----------

Access task configuration:

.. code-block:: jinja

    {{ task.name }}
    {{ task.commandline }}
    {{ task.run_dir }}

Cycle Variables
===============

When No Cycles
--------------

If workflow has no cycles or single cycle:

.. code-block:: jinja

    {{ cycle }}           # None or fixed date

When Using Cycles
-----------------

**Date Information:**

.. code-block:: jinja

    {{ cycle.date }}          # Point-in-time date
    {{ cycle.begin_date }}    # Interval start
    {{ cycle.end_date }}      # Interval end

**Formatted Dates:**

.. code-block:: jinja

    {{ cycle.date_str }}           # "2020-01-01T00:00:00"
    {{ cycle.begin_date_str }}     # "2020-01-01T00:00:00"
    {{ cycle.end_date_str }}       # "2020-01-02T00:00:00"

**Tokens:**

.. code-block:: jinja

    {{ cycle.token }}         # "2020-01-01T00:00:00-2020-01-02T00:00:00"
    {{ cycle.label }}         # Same as token

**Boolean:**

.. code-block:: jinja

    {{ cycle.is_interval }}   # True if interval, False if point

**Example - Point Cycle:**

Configuration:

.. code-block:: ini

    [cycles]
    begin_date = 2020-06-15
    as_intervals = False

Template:

.. code-block:: jinja

    Date: {{ cycle.date }}
    Formatted: {{ cycle.date_str }}
    Token: {{ cycle.token }}

Result:

.. code-block:: text

    Date: 2020-06-15 00:00:00
    Formatted: 2020-06-15T00:00:00
    Token: 2020-06-15T00:00:00

**Example - Interval Cycle:**

Configuration:

.. code-block:: ini

    [cycles]
    begin_date = 2020-01-01
    freq = 1D
    as_intervals = True

Template:

.. code-block:: jinja

    Period: {{ cycle.begin_date }} to {{ cycle.end_date }}
    Start: {{ cycle.begin_date_str }}
    End: {{ cycle.end_date_str }}
    ID: {{ cycle.token }}

Result (for first cycle):

.. code-block:: text

    Period: 2020-01-01 00:00:00 to 2020-01-02 00:00:00
    Start: 2020-01-01T00:00:00
    End: 2020-01-02T00:00:00
    ID: 2020-01-01T00:00:00-2020-01-02T00:00:00

Cycle Equality and Comparison
------------------------------

Cycles can be compared for equality using the ``==`` operator. The comparison behavior is designed to be flexible and intuitive for workflow operations.

**Equality Rules:**

1. **Same type cycles**: Two cycles are equal if they have the same ``begin_date`` and ``end_date``
2. **Mixed type cycles**: A single date cycle equals an interval cycle if they share the same ``begin_date``
3. **String comparison**: Cycles can be compared with ISO 8601 formatted strings

**Example 1: Same Type Comparison**

.. code-block:: python

    from woom.iters import Cycle

    # Two single date cycles
    cycle1 = Cycle("2020-01-01")
    cycle2 = Cycle("2020-01-01")
    assert cycle1 == cycle2  # True - same date

    cycle3 = Cycle("2020-01-02")
    assert cycle1 != cycle3  # True - different dates

    # Two interval cycles
    interval1 = Cycle("2020-01-01", "2020-01-10")
    interval2 = Cycle("2020-01-01", "2020-01-10")
    assert interval1 == interval2  # True - same interval

    interval3 = Cycle("2020-01-01", "2020-01-15")
    assert interval1 != interval3  # True - different end dates

**Example 2: Mixed Type Comparison**

.. code-block:: python

    # Single date vs interval - compared by begin_date
    single = Cycle("2020-01-01")
    interval = Cycle("2020-01-01", "2020-01-10")
    assert single == interval  # True - same begin_date

    # Different begin dates
    single2 = Cycle("2020-01-02")
    assert single2 != interval  # True - different begin_date

**Example 3: String Comparison**

.. code-block:: python

    cycle = Cycle("2020-01-01")
    assert cycle == "2020-01-01T00:00:00+00:00"  # True

    interval = Cycle("2020-01-01", "2020-01-10")
    assert interval == "2020-01-01T00:00:00+00:00-2020-01-10T00:00:00+00:00"  # True

**Use Cases:**

This flexible equality behavior is useful for:

- **Task dependencies**: Tasks can depend on cycles specified as dates or intervals interchangeably
- **Cycle lookups**: Find cycles by date string without knowing if it's stored as interval or point
- **Workflow logic**: Simplify conditional logic when cycles might be configured differently

.. code-block:: python

    # In workflow code, these all work equivalently
    task_cycle = workflow.get_task_cycle(task_name, "2020-01-01")

    # Whether the workflow uses:
    # - Single dates: Cycle("2020-01-01")
    # - Intervals: Cycle("2020-01-01", "2020-01-02")
    # Both match the string "2020-01-01"

Ensemble Member Variables
==========================

When No Ensemble
----------------

If no ensemble configured:

.. code-block:: jinja

    {{ member }}    # None

When Using Ensemble
-------------------

**Basic Information:**

.. code-block:: jinja

    {{ member.id }}       # Integer: 1, 2, 3, ...
    {{ member.label }}    # String: "member001", "member002", ...

**Custom Parameters:**

From ``[[iters]]`` section:

.. code-block:: ini

    [ensemble]
    size = 3
        [[iters]]
        temperature = 10, 15, 20
        scenario = cold, medium, warm

Template:

.. code-block:: jinja

    Member: {{ member.id }}
    Label: {{ member.label }}
    Temperature: {{ member.temperature }}
    Scenario: {{ member.scenario }}

Results for member 2:

.. code-block:: text

    Member: 2
    Label: member002
    Temperature: 15
    Scenario: medium

Custom Parameters
=================

Global Parameters
-----------------

Defined in ``[params]`` section of workflow.cfg.

**Flat parameters** (use underscores):

.. code-block:: ini

    [params]
    timestep = 300
    grid_nx = 100
    grid_ny = 200

Template:

.. code-block:: jinja

    dt = {{ params.timestep }}
    nx = {{ params.grid_nx }}
    ny = {{ params.grid_ny }}

**Nested parameters** (use ConfigObj subsections):

.. code-block:: ini

    [params]
    timestep = 300

        [[paths]]
        data_dir = /data/ocean
        forcing_dir = /data/forcing

Template:

.. code-block:: jinja

    dt = {{ params.timestep }}
    data: {{ params.paths.data_dir }}
    forcing: {{ params.paths.forcing_dir }}

Host-Specific Parameters
-------------------------

Override per host:

.. code-block:: ini

    [params]
    nprocs = 128

        [[hosts]]
            [[[laptop]]]
            nprocs = 4

            [[[supercomputer]]]
            nprocs = 2048

Template uses appropriate value:

.. code-block:: jinja

    mpirun -n {{ params.nprocs }} ./model

Task-Specific Parameters
-------------------------

Override per task:

.. code-block:: ini

    [params]
    memory = 16GB

        [[tasks]]
            [[[run_model]]]
            memory = 128GB

            [[[postprocess]]]
            memory = 32GB

Environment Variables
=====================

System Variables
----------------

Standard environment variables:

.. code-block:: jinja

    {{ task_env.USER }}
    {{ task_env.HOME }}
    {{ task_env.HOSTNAME }}
    {{ task_env.PWD }}

Workflow Variables
------------------

Set in ``[env_vars]`` section:

.. code-block:: ini

    [env_vars]
    DATA_ROOT = /data/ocean
    OMP_NUM_THREADS = 8

Access:

.. code-block:: jinja

    {{ task_env.DATA_ROOT }}
    {{ task_env.OMP_NUM_THREADS }}

Special Woom Variables
-----------------------

Automatically set by woom:

.. code-block:: jinja

    {{ task_env.WOOM_WORKFLOW_DIR }}
    {{ task_env.WOOM_TASK_NAME }}
    {{ task_env.WOOM_RUN_DIR }}
    {{ task_env.WOOM_CYCLE }}
    {{ task_env.WOOM_MEMBER }}

Host Variables
==============

Scratch Directory
-----------------

.. code-block:: jinja

    {{ scratch_dir }}    # From host configuration

Useful for dynamic paths:

.. code-block:: jinja

    run_dir = {{ scratch_dir }}/{{ task_path }}

Complete Context Example
=========================

Given this configuration:

**workflow.cfg:**

.. code-block:: ini

    [app]
    name = ocean_model
    conf = tropical
    exp = test01

    [cycles]
    begin_date = 2020-01-01
    freq = 1D
    ncycles = 2
    as_intervals = True

    [ensemble]
    size = 2
        [[iters]]
        perturbation = 0.01, 0.02

    [params]
    timestep = 600
        [[paths]]
        output_base = /scratch/output

**Template:**

.. code-block:: jinja

    # Configuration for {{ app.name }}/{{ app.conf }}/{{ app.exp }}
    # Task: {{ task.name }}
    # Cycle: {{ cycle.begin_date }} to {{ cycle.end_date }}
    # Member: {{ member.label }}

    [simulation]
    start_time = "{{ cycle.begin_date_str }}"
    end_time = "{{ cycle.end_date_str }}"
    timestep = {{ params.timestep }}
    perturbation = {{ member.perturbation }}

    [output]
    directory = "{{ params.paths.output_base }}/{{ task_path }}"
    filename = "output_{{ cycle.token }}_{{ member.label }}.nc"

**Result** (task=run_model, cycle 1, member 2):

.. code-block:: ini

    # Configuration for ocean_model/tropical/test01
    # Task: run_model
    # Cycle: 2020-01-01 00:00:00 to 2020-01-02 00:00:00
    # Member: member002

    [simulation]
    start_time = "2020-01-01T00:00:00"
    end_time = "2020-01-02T00:00:00"
    timestep = 600
    perturbation = 0.02

    [output]
    directory = "/scratch/output/ocean_model/tropical/test01/2020-01-01T00:00:00-2020-01-02T00:00:00/run_model/member002"
    filename = "output_2020-01-01T00:00:00-2020-01-02T00:00:00_member002.nc"

Advanced Techniques
===================

Conditional Content
-------------------

Use Jinja2 conditionals:

.. code-block:: jinja

    {% if cycle %}
    # Cycle-specific settings
    start_date = "{{ cycle.begin_date_str }}"
    {% else %}
    # No cycle - use default
    start_date = "2020-01-01T00:00:00"
    {% endif %}

    {% if member %}
    # Ensemble member {{ member.id }}
    seed = {{ 1000 + member.id }}
    {% else %}
    # Single run
    seed = 1000
    {% endif %}

Loops
-----

Loop over collections:

.. code-block:: jinja

    # All ensemble members
    {% if member is not none %}
    Members:
    {% for m in workflow.members %}
    - {{ m.label }}: {{ m.perturbation }}
    {% endfor %}
    {% endif %}

Filters
-------

Apply Jinja2 filters:

.. code-block:: jinja

    # Uppercase
    Model: {{ app.name | upper }}

    # Path join
    File: {{ [run_dir, 'output.nc'] | join('/') }}

    # Default value
    Value: {{ params.optional | default('default_value') }}

Custom Filters
--------------

Register custom filters in extension files:

.. code-block:: python

    # ext/jinja_filters.py
    from woom.ext import JINJA_FILTERS

    def format_date(date, fmt='%Y%m%d'):
        return date.strftime(fmt)

    JINJA_FILTERS['format_date'] = format_date

Use in templates:

.. code-block:: jinja

    Date: {{ cycle.date | format_date('%Y-%m-%d') }}

Accessing Context Programmatically
===================================

From Python
-----------

.. code-block:: python

    # Get context for specific task/cycle/member
    context = workflow.get_context(
        task_name='run_model',
        cycle='2020-01-01',
        member=1
    )

    # Access variables
    print(f"Task: {context['task_name']}")
    print(f"Cycle: {context['cycle'].date}")
    print(f"Member: {context['member'].id}")
    print(f"Parameter: {context['params']['timestep']}")

    # Set as active context
    workflow.set_context(task_name='run_model', cycle='2020-01-01')

    # Now use workflow.context
    task_name = workflow.context['task_name']

Context Scope
-------------

Context is task-specific:

.. code-block:: python

    # Different contexts for different tasks
    ctx1 = workflow.get_context(task_name='task1', cycle='2020-01-01')
    ctx2 = workflow.get_context(task_name='task2', cycle='2020-01-01')

    # Same cycle, different task info
    assert ctx1['task_name'] == 'task1'
    assert ctx2['task_name'] == 'task2'

Best Practices
==============

1. **Use Descriptive Variable Names**: ``{{ params.model.timestep }}`` is clearer than ``{{ params.dt }}``

2. **Provide Defaults**: Use ``{{ var | default('fallback') }}`` for optional variables

3. **Document Parameters**: Comment what each parameter controls in your configuration

4. **Organize Parameters**: Use nested sections in ``[params]`` for logical grouping

5. **Test Templates**: Render templates independently to verify output

6. **Handle Missing Cycles/Members**: Check for None before accessing cycle/member attributes

7. **Keep It Simple**: Don't overuse complex Jinja2 logic - consider Python preprocessing instead

8. **Consistent Naming**: Use consistent naming conventions across workflow, tasks, and parameters

Common Patterns
===============

Date-Based Output Paths
------------------------

.. code-block:: jinja

    output = {{ scratch_dir }}/{{ app.name }}/{{ cycle.date | strftime('%Y/%m/%d') }}/output.nc

Conditional Paths
-----------------

.. code-block:: jinja

    {% if member %}
    output = results_{{ cycle.token }}_{{ member.label }}.nc
    {% else %}
    output = results_{{ cycle.token }}.nc
    {% endif %}

Resource Scaling
----------------

.. code-block:: jinja

    {% if params.resolution == 'high' %}
    mpirun -n 512 ./model
    {% elif params.resolution == 'medium' %}
    mpirun -n 128 ./model
    {% else %}
    mpirun -n 32 ./model
    {% endif %}

Troubleshooting
===============

Undefined Variable
------------------

Error: ``UndefinedError: 'cycle' is undefined``

**Cause**: Accessing variable that doesn't exist in current context

**Fix**: Check if variable exists or provide default:

.. code-block:: jinja

    {% if cycle %}{{ cycle.date }}{% else %}No cycle{% endif %}
    {{ cycle.date | default('N/A') }}

Wrong Variable Type
-------------------

Error: ``AttributeError: 'NoneType' object has no attribute 'date'``

**Cause**: Variable is None

**Fix**: Check before accessing attributes:

.. code-block:: jinja

    {% if cycle is not none %}
    Date: {{ cycle.date }}
    {% endif %}

Template Not Rendering
-----------------------

**Cause**: Syntax errors, missing variables

**Debug**:
1. Test template with simple content first
2. Add variables incrementally
3. Check Jinja2 syntax
4. Verify context has expected variables

Migration Guide
===============

This section helps migrate workflows from older Woom versions to the current version.

Parameters Isolation (v2025.10+)
---------------------------------

**Change**: User-defined workflow parameters are now isolated in the ``params`` sub-dictionary.

**Old Syntax** (before v2025.10):

.. code-block:: jinja

    # Direct access to parameters
    dt = {{ timestep }}
    nx = {{ grid_nx }}
    data = {{ data_dir }}

**New Syntax** (v2025.10+):

.. code-block:: jinja

    # Parameters accessed via params
    dt = {{ params.timestep }}
    nx = {{ params.grid_nx }}
    data = {{ params.data_dir }}

**Why**: This change prevents naming conflicts between user parameters and built-in context variables, making templates more predictable and maintainable.

**Migration Steps**:

1. Add ``params.`` prefix to all user-defined parameters in templates
2. Environment variables also change: ``WOOM_TIMESTEP`` becomes ``WOOM_PARAMS_TIMESTEP``
3. Test all templates after migration

Task Variable Prefixes (v2025.10+)
-----------------------------------

**Change**: Task-specific context variables now use the ``task_`` prefix.

**Old Syntax** (before v2025.10):

.. code-block:: jinja

    Working directory: {{ run_dir }}
    Submission directory: {{ submission_dir }}
    Script path: {{ script_path }}
    Environment: {{ env.USER }}
    Context JSON: {{ context_json }}

**New Syntax** (v2025.10+):

.. code-block:: jinja

    Working directory: {{ task_run_dir }}
    Submission directory: {{ task_submission_dir }}
    Script path: {{ task_script_path }}
    Environment: {{ task_env.USER }}
    Context JSON: {{ task_context_json }}

**Why**: The ``task_`` prefix makes it explicit that these variables are task-specific, improving template readability and avoiding ambiguity with workflow-level variables.

**Migration Steps**:

1. Replace ``{{ run_dir }}`` with ``{{ task_run_dir }}``
2. Replace ``{{ submission_dir }}`` with ``{{ task_submission_dir }}``
3. Replace ``{{ script_path }}`` with ``{{ task_script_path }}``
4. Replace ``{{ env.`` with ``{{ task_env.``
5. Replace ``{{ context_json }}`` with ``{{ task_context_json }}``

**Backward Compatibility**: For a limited time, the old names remain available as aliases to the new names, but they will be removed in a future version. Migrate as soon as possible.

Complete Migration Example
---------------------------

**Old template** (before v2025.10):

.. code-block:: jinja

    #!/bin/bash
    # Model configuration
    cd {{ run_dir }}

    export MODEL_TIMESTEP={{ timestep }}
    export MODEL_GRIDSIZE={{ grid_nx }}x{{ grid_ny }}
    export DATA_DIR={{ data_dir }}
    export USER={{ env.USER }}

    echo "Running in: $(pwd)"
    echo "Job script: {{ script_path }}"
    ./model --config config.nml

**New template** (v2025.10+):

.. code-block:: jinja

    #!/bin/bash
    # Model configuration
    cd {{ task_run_dir }}

    export MODEL_TIMESTEP={{ params.timestep }}
    export MODEL_GRIDSIZE={{ params.grid_nx }}x{{ params.grid_ny }}
    export DATA_DIR={{ params.data_dir }}
    export USER={{ task_env.USER }}

    echo "Running in: $(pwd)"
    echo "Job script: {{ task_script_path }}"
    ./model --config config.nml

See Also
========

- :ref:`indepth.templating` - Using templates with context
- :ref:`inputs_context` - Context variable reference
- :ref:`inputs_envvars` - Environment variables
