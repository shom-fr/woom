{% extends "!job.sh" %}

{% block header %}
{{ super() }}
# Workflow: {{ app_name|default('N/A') }}
# Configuration: {{ app_conf|default('N/A') }}
# Experiment: {{ app_exp|default('N/A') }}
# Task: {{ task.name }}
{% endblock %}
