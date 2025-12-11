{% extends "example.rst" %}

{% block configuring %}
{{ super() }}

Extending
---------

.. literalinclude:: {{ workflow_dir }}/ext/artifacts_generators.py
    :start-at: import
    :caption: :file:`ext/artifacts_generators.py`
{% endblock %}

{% block epilog %}
Show artifacts
~~~~~~~~~~~~~~
Show artifacts, either generated or expected.

.. command-output:: woom show artifacts
    :cwd: {{ workflow_dir }}
{% endblock %}
