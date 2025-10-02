{% extends "example.rst" %}

{% block epilog %}
Show artifacts
~~~~~~~~~~~~~~
Show artifacts, either generated or expected.

.. command-output:: woom show artifacts
    :cwd: {{ workflow_dir }}
{% endblock %}
