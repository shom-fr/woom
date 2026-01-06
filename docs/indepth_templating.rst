.. _indepth.templating:

Templating In-Depth
###################

Woom uses Jinja2 templating to create dynamic, reusable workflows. Templates allow you to generate configuration files, namelists, and scripts that adapt to different tasks, cycles, ensemble members, and environments.

Overview
========

Templating in woom occurs at two levels:

1. **System Templates**: Job scripts that woom uses internally
2. **User Templates**: Configuration files and scripts you create

Both use the same Jinja2 syntax and have access to the same context variables.

Jinja2 Basics
=============

Variables
---------

Insert variable values with ``{{ }}``:

.. code-block:: jinja

    Task name: {{ task.name }}
    Cycle: {{ cycle.begin_date }}
    Parameter: {{ params.timestep }}

Expressions
-----------

Evaluate expressions:

.. code-block:: jinja

    Processors: {{ params.nodes * params.procs_per_node }}
    Output: result_{{ cycle.date.year }}_{{ cycle.date.month }}.nc
    Path: {{ [run_dir, 'output', 'data.nc'] | join('/') }}

Comments
--------

Add comments (not in output):

.. code-block:: jinja

    {# This is a comment #}
    {# TODO: Update this section #}

Conditionals
------------

.. code-block:: jinja

    {% if cycle %}
    start_date = "{{ cycle.begin_date_str }}"
    {% else %}
    start_date = "2020-01-01T00:00:00"
    {% endif %}

    {% if member %}
    member_id = {{ member.id }}
    {% endif %}

Loops
-----

.. code-block:: jinja

    {% for i in range(1, 11) %}
    file_{{ i }} = data_{{ '%03d' % i }}.nc
    {% endfor %}

Filters
-------

Transform values:

.. code-block:: jinja

    {{ app.name | upper }}
    {{ path | basename }}
    {{ value | default('N/A') }}
    {{ items | join(', ') }}

System Templates
================

Job Script Template
-------------------

The default :file:`job.sh` template generates batch scripts:

**Location**: :file:`woom/templates/job.sh`

**Key Sections**:

- Shebang and shell settings
- Scheduler directives
- Environment setup
- Command execution
- Status tracking

**Customization**:

Create :file:`templates/job.sh` in your workflow directory to override.

Environment Template
--------------------

The :file:`env.sh` template handles environment setup:

**Location**: :file:`woom/templates/env.sh`

**Purpose**:

- Load modules
- Activate virtual environments
- Set environment variables
- Source setup scripts

User Templates
==============

Creating Templates
------------------

1. Create :file:`templates/` directory in workflow directory
2. Add template files with :file:`.j2` extension
3. Use Jinja2 syntax with context variables
4. Configure in tasks.cfg ``[[fill]]`` section

Directory Structure
-------------------

.. code-block:: text

    my_workflow/
    ├── workflow.cfg
    ├── tasks.cfg
    ├── hosts.cfg
    └── templates/
        ├── model_config.xml.j2
        ├── namelist.nml.j2
        ├── forcing_list.txt.j2
        └── post_process.sh.j2

Template Examples
=================

Example 1: Model Namelist
--------------------------

**templates/ocean.nml.j2:**

.. code-block:: jinja

    &time_control
        title = "{{ app.name }} - {{ app.exp }}"
        start_date = "{{ cycle.begin_date_str }}"
        end_date = "{{ cycle.end_date_str }}"
        dt = {{ params.timestep }}
        time_stepping = "{{ params.time_scheme }}"
    /

    &grid
        nx = {{ params.grid_nx }}
        ny = {{ params.grid_ny }}
        nz = {{ params.grid_nz }}
        dx = {{ params.grid_resolution }}
    /

    &physics
        {# Different physics for different scenarios #}
        {% if params.scenario == 'realistic' %}
        viscosity = 100.0
        diffusivity = 50.0
        {% else %}
        viscosity = 1000.0
        diffusivity = 500.0
        {% endif %}
    /

    &output
        output_dir = "{{ task_run_dir }}/output"
        output_freq = {{ params.output_frequency }}
        {% if member %}
        output_file = "ocean_{{ cycle.token }}_{{ member.label }}.nc"
        {% else %}
        output_file = "ocean_{{ cycle.token }}.nc"
        {% endif %}
    /

**tasks.cfg:**

.. code-block:: ini

    [run_ocean]
        [[fill]]
            [[[namelist]]]
            template = ocean.nml.j2
            destination = {{ task_run_dir }}/ocean.nml

Example 2: XML Configuration
-----------------------------

**templates/model.xml.j2:**

.. code-block:: jinja

    <?xml version="1.0"?>
    <configuration>
        <experiment>
            <name>{{ app.name }}</name>
            <description>{{ params.description | default('No description') }}</description>
            {% if cycle %}
            <start_date>{{ cycle.begin_date_str }}</start_date>
            <end_date>{{ cycle.end_date_str }}</end_date>
            {% endif %}
        </experiment>

        <domain>
            <nx>{{ params.domain.nx }}</nx>
            <ny>{{ params.domain.ny }}</ny>
            <resolution>{{ params.domain.resolution }}</resolution>
        </domain>

        <forcing>
            {% for forcing_type in params.forcing_types %}
            <input type="{{ forcing_type }}">
                <path>{{ params.forcing_dir }}/{{ forcing_type }}_{{ cycle.token }}.nc</path>
            </input>
            {% endfor %}
        </forcing>

        {% if member %}
        <ensemble>
            <member_id>{{ member.id }}</member_id>
            <perturbation>{{ member.perturbation }}</perturbation>
        </ensemble>
        {% endif %}
    </configuration>

Example 3: File List
--------------------

**templates/input_files.txt.j2:**

.. code-block:: jinja

    # Input files for {{ task.name }}
    # Generated: {{ now }}

    # Bathymetry
    {{ params.static_data }}/bathymetry.nc

    # Initial conditions
    {% if cycle.is_first == 1 %}
    {{ params.initial_conditions }}/ic_default.nc
    {% else %}
    {# Use restart from previous cycle #}
    {{ previous_cycle_dir }}/restart.nc
    {% endif %}

    # Forcing files
    {% for day in range(cycle.begin_date.day, cycle.end_date.day) %}
    {{ params.forcing_dir }}/forcing_{{ cycle.begin_date.year }}{{ cycle.begin_date.month | string | rjust(2, '0') }}{{ day | string | rjust(2, '0') }}.nc
    {% endfor %}

    # Output directory
    {{ task_run_dir }}/output

Example 4: Shell Script
-----------------------

**templates/post_process.sh.j2:**

.. code-block:: jinja

    #!/bin/bash
    # Post-processing for {{ task.name }}
    # Cycle: {{ cycle.token if cycle else 'No cycle' }}

    set -e
    set -u

    # Directories
    INPUT_DIR="{{ task_run_dir }}/output"
    OUTPUT_DIR="{{ scratch_dir }}/processed/{{ task_path }}"
    mkdir -p "${OUTPUT_DIR}"

    # Process outputs
    {% if member %}
    # Ensemble member {{ member.id }}
    INPUT_FILE="ocean_{{ cycle.token }}_{{ member.label }}.nc"
    OUTPUT_FILE="processed_{{ cycle.token }}_{{ member.label }}.nc"
    {% else %}
    INPUT_FILE="ocean_{{ cycle.token }}.nc"
    OUTPUT_FILE="processed_{{ cycle.token }}.nc"
    {% endif %}

    echo "Processing: ${INPUT_FILE}"
    python {{ workflow_dir }}/scripts/process.py \
        --input "${INPUT_DIR}/${INPUT_FILE}" \
        --output "${OUTPUT_DIR}/${OUTPUT_FILE}" \
        --param {{ params.process_param }}

    echo "Done: ${OUTPUT_FILE}"

Advanced Techniques
===================

Macros
------

Define reusable template sections:

.. code-block:: jinja

    {% macro grid_section(nx, ny, nz, dx) %}
    &grid
        nx = {{ nx }}
        ny = {{ ny }}
        nz = {{ nz }}
        dx = {{ dx }}
    /
    {% endmacro %}

    {# Use the macro #}
    {{ grid_section(params.grid_nx, params.grid_ny, params.grid_nz, params.grid_resolution) }}

Template Inheritance
--------------------

Create base templates:

**templates/base_config.j2:**

.. code-block:: jinja

    [general]
    experiment = {{ app.exp }}

    {% block simulation %}
    {# Override this block #}
    {% endblock %}

    {% block output %}
    output_dir = {{ task_run_dir }}/output
    {% endblock %}

**templates/hindcast.j2:**

.. code-block:: jinja

    {% extends "base_config.j2" %}

    {% block simulation %}
    mode = hindcast
    start = {{ cycle.begin_date_str }}
    end = {{ cycle.end_date_str }}
    {% endblock %}

Includes
--------

Reuse template fragments:

**templates/common_params.j2:**

.. code-block:: jinja

    timestep = {{ params.timestep }}
    output_freq = {{ params.output_freq }}

**templates/model.cfg.j2:**

.. code-block:: jinja

    [configuration]
    {% include 'common_params.j2' %}

    [specific]
    custom_param = {{ params.custom }}

Whitespace Control
------------------

Control whitespace in output:

.. code-block:: jinja

    {% for item in items -%}
    {{ item }}
    {%- endfor %}

    {#- Minus sign removes whitespace -#}

Custom Filters and Functions
=============================

Custom Filters
--------------

**ext/jinja_filters.py:**

.. code-block:: python

    from woom.ext import JINJA_FILTERS
    import os

    def basename(path):
        """Get filename from path"""
        return os.path.basename(path)

    def format_date(date, fmt='%Y%m%d'):
        """Format date object"""
        return date.strftime(fmt)

    def pad_zeros(num, width=3):
        """Pad number with zeros"""
        return str(num).zfill(width)

    # Register filters
    JINJA_FILTERS['basename'] = basename
    JINJA_FILTERS['format_date'] = format_date
    JINJA_FILTERS['pad'] = pad_zeros

**Usage:**

.. code-block:: jinja

    File: {{ full_path | basename }}
    Date: {{ cycle.date | format_date('%Y-%m-%d') }}
    Member: member{{ member.id | pad(3) }}

Custom Tests
------------

**ext/jinja_filters.py:**

.. code-block:: python

    from woom.ext import JINJA_TESTS

    def is_high_res(resolution):
        """Test if resolution is high"""
        return float(resolution.replace('km', '')) < 5

    JINJA_TESTS['high_res'] = is_high_res

**Usage:**

.. code-block:: jinja

    {% if params.resolution is high_res %}
    # High resolution settings
    timestep = 60
    {% else %}
    # Standard resolution
    timestep = 300
    {% endif %}

Global Variables
----------------

Add variables available to all templates:

**ext/jinja_filters.py:**

.. code-block:: python

    from woom.ext import JINJA_GLOBALS
    import datetime

    JINJA_GLOBALS['now'] = datetime.datetime.now()
    JINJA_GLOBALS['pi'] = 3.14159

**Usage:**

.. code-block:: jinja

    # Generated: {{ now }}
    # π = {{ pi }}

Template Debugging
==================

Print Variable
--------------

.. code-block:: jinja

    {# Debug: see what's in params #}
    {{ params }}

    {# Pretty print #}
    <pre>{{ params | pprint }}</pre>

Conditional Debug
-----------------

.. code-block:: jinja

    {% if env.DEBUG | default(false) %}
    # Debug information
    Task: {{ task.name }}
    Cycle: {{ cycle }}
    Member: {{ member }}
    All params: {{ params }}
    {% endif %}

Test Rendering
--------------

Use ``woom fill`` to test templates:

.. code-block:: bash

    woom fill my_template.j2 output.txt --dry-run

Best Practices
==============

1. **Use Comments**: Document template logic for future reference

2. **Validate Syntax**: Test templates before using in production

3. **Handle None Values**: Check if variables exist before using

4. **Keep Templates Simple**: Move complex logic to Python code

5. **Use Meaningful Names**: Clear variable and file names

6. **Version Control**: Track template changes in git

7. **Test Edge Cases**: Test with/without cycles, members, etc.

8. **Provide Defaults**: Use ``| default()`` for optional values

9. **Organize Templates**: Group related templates in subdirectories

10. **Document Variables**: Add comments showing what variables are needed

Common Patterns
===============

Conditional Sections
--------------------

.. code-block:: jinja

    [basic_config]
    param = value

    {% if advanced_mode %}
    [advanced_config]
    param1 = {{ advanced.param1 }}
    param2 = {{ advanced.param2 }}
    {% endif %}

Environment-Specific
--------------------

.. code-block:: jinja

    {% if hostname == 'supercomputer' %}
    mpirun -n 2048 ./model
    {% elif hostname == 'workstation' %}
    mpirun -n 16 ./model
    {% else %}
    ./model
    {% endif %}

Loop with Conditions
--------------------

.. code-block:: jinja

    {% for item in items %}
        {% if item.enabled %}
    {{ item.name }} = {{ item.value }}
        {% endif %}
    {% endfor %}

Date Calculations
-----------------

.. code-block:: jinja

    {% set days = (cycle.end_date - cycle.begin_date).days %}
    Duration: {{ days }} days

Troubleshooting
===============

Syntax Errors
-------------

Error: ``TemplateSyntaxError``

**Common Causes:**

- Mismatched ``{% %}`` tags
- Missing ``{% endif %}``, ``{% endfor %}``
- Invalid Jinja2 syntax

**Fix**: Check template syntax, match opening/closing tags

Undefined Variables
-------------------

Error: ``UndefinedError: 'variable' is undefined``

**Fix**:

.. code-block:: jinja

    {# Check if exists #}
    {% if variable is defined %}
    value = {{ variable }}
    {% endif %}

    {# Or provide default #}
    value = {{ variable | default('fallback') }}

Wrong Output
------------

**Cause**: Variables not rendering as expected

**Debug**:

1. Print variable: ``{{ variable }}``
2. Check type: ``{{ variable.__class__.__name__ }}``
3. Test with simple template first
4. Verify context has expected values

Template Not Found
------------------

Error: ``TemplateNotFound: template.j2``

**Fix**:

- Ensure template is in :file:`templates/` directory
- Check filename spelling
- Verify file extension is included in template path

See Also
========

- :ref:`indepth.context` - Variables available in templates
- :ref:`templates` - Default templates
- :ref:`ext` - Extension system for custom filters
- `Jinja2 Documentation <https://jinja.palletsprojects.com/>`_ - Full Jinja2 reference
