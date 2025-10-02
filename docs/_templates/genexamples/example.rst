{% set section = os.path.basename(os.path.dirname(abs_workflow_dir)) %}
{% set name = os.path.basename(abs_workflow_dir) %}


.. _examples.{{ section  }}.{{ name }}:

{% block readme -%}
.. include:: {{ os.path.join(workflow_dir, "README.rst") }}

Path: :file:`examples/{{ section }}/{{ name }}`.
{% endblock %}

{% block configuring %}
Configuring
-----------

{% block workflow_cfg %}
{% if os.path.exists(os.path.join(abs_workflow_dir, "workflow.cfg")) %}
.. literalinclude:: {{ os.path.join(workflow_dir, "workflow.cfg") }}
    :language: ini
    :caption: :file:`workflow.cfg`
{% endif %}
{% endblock %}

{% block tasks_cfg %}
{% if os.path.exists(os.path.join(abs_workflow_dir, "tasks.cfg")) %}
.. literalinclude:: {{ os.path.join(workflow_dir, "tasks.cfg") }}
    :language: ini
    :caption: :file:`tasks.cfg`
{% endif %}
{% endblock %}

{% block hosts_cfg %}
{% if os.path.exists(os.path.join(abs_workflow_dir, "hosts.cfg")) %}
.. literalinclude:: {{ os.path.join(workflow_dir, "hosts.cfg") }}
    :language: ini
    :caption: :file:`hosts.cfg`
{% endif %}
{% endblock %}

{% endblock %}

{% block running %}
Running
-------

{% block overview -%}
Overview
~~~~~~~~
Let's have an overview of stages before running the workflow.

.. command-output:: woom show overview
    :cwd: {{ workflow_dir }}
{% endblock %}

{% block dry_run -%}
Dry run
~~~~~~~
Now let's run the workflow in test (dry) and debug modes.

.. command-output:: woom run --log-no-color --log-level debug --dry-run
    :cwd: {{ workflow_dir }}
{% endblock %}

{% if section == "academic" %}

{% block normal_run -%}
Normal run
~~~~~~~~~~
And finally in run it.

.. command-output:: woom run --log-no-color
    :cwd: {{ workflow_dir }}
{% endblock %}

{% block check_status -%}
Check status
~~~~~~~~~~~~
Check what is running or finished.

.. command-output:: woom show status
    :cwd: {{ workflow_dir }}
{% endblock %}

{% block show_run_directories -%}
Show run directories
~~~~~~~~~~~~~~~~~~~~
Show where tasks were executed.

.. command-output:: woom show run_dirs
    :cwd: {{ workflow_dir }}
{% endblock %}

{% endif %}
{% endblock %}

{% block epilog -%}
{% endblock %}
