{% block header -%}
#!/bin/bash

# Prolog
set -eo pipefail
# - handler for graceful termination
on_term() {
    echo "Received termination signal, cleaning up..." >&2
    # Just exit cleanly, let on_exit handle status
    exit 0
}
# - handler for exit (always called)
on_exit() {
    status=$?
    echo $status > "{{ submission_dir }}/job.status"
    exit $status
}
trap on_term SIGTERM SIGINT
trap on_exit EXIT
{% endblock %}

{% block env -%}
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
{% for name, paths in task.artifacts.items() -%}
  {% for path in paths -%}
test -f "{{ path }}" || { echo artifact not created: {{ name }}={{ path }}; exit 1; }
  {% endfor %}
{% endfor %}
{% endif %}
{% endblock %}
