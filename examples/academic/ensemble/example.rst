{% extends "example.rst" %}

{% block configuring %}
{{ super() }}

Extending
---------


.. literalinclude:: {{ workflow_dir }}/ext/jinja_filters.py
    :start-at: import
    :caption: :file:`ext/jinja_filters.py`

.. literalinclude:: {{ workflow_dir }}/workflow.ini
    :language: ini
    :caption: :file:`workflow.ini`

.. literalinclude:: {{ workflow_dir }}/ext/validator_functions.py
    :start-at: import
    :caption: :file:`ext/validator_functions.py`
{% endblock %}
