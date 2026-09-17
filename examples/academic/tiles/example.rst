{% extends "example.rst" %}

{% block configuring %}
{{ super() }}

Extensions
----------

.. literalinclude:: {{ workflow_dir }}/workflow.ini
    :language: ini
    :caption: :file:`workflow.ini`

.. literalinclude:: {{ workflow_dir }}/ext/validator_functions.py
    :start-at: import
    :caption: :file:`ext/validator_functions.py`

.. literalinclude:: {{ workflow_dir }}/bin/process_tile.py
    :start-at: import
    :caption: :file:`bin/process_tile.py`
{% endblock %}
