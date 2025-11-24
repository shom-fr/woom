{% block header -%}
#!/bin/bash

# Prolog
set -eo pipefail
on_exit() {
    status=$?
    echo $status > "{{ submission_dir }}/job.status"
    exit $status
}
trap on_exit EXIT
{% endblock %}

{% block env -%}
{# task.export_env(params) #}
{% include "env.sh" %}
{% endblock %}

{% block pre_run -%}
{% if run_dir %}
# Go to run dir
{ mkdir -p {{ run_dir }}; cd {{ run_dir }}; } || exit 1
{% endif %}
{% endblock %}

{% block run -%}
# Run the commandline(s)
{{ task.commandline }}
{% endblock %}

{% block post_run -%}
{% if task.artifacts %}
# Check artifacts
{# task.export_artifacts_checking() #}
{% for name, paths in task.artifacts.items() -%}
  {% for path in paths -%}
test -f "{{ path }}" || { echo artifact not created: {{ name }}={{ path }}; exit 1; }
  {% endfor %}
{% endfor %}

{% endif %}
{% endblock %}
