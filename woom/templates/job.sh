{% block header -%}
#!/bin/bash

# Prolog
set -eo pipefail
# - handler for graceful termination
on_sigterm() {
    echo "Received termination signal, cleaning up..."
    # Just exit cleanly, let on_exit handle status
    exit 0
}
# - handler for killing termination
on_sigkill() {
    echo "Received kill signal, cleaning up..." >&2
    # Just exit cleanly, let on_exit handle status
    exit 1
}
# - handler for exit (always called)
on_exit() {
    status=$?
    if [ ! -f "{{ submission_dir }}/job.terminating" ]; then
        echo $status > "{{ submission_dir }}/job.status"
    fi
    exit $status
}
trap on_sigterm SIGKILL
trap on_sigkill SIGTERM SIGINT
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
{% for name, path in task.artifacts.items() -%}
  {% if path is string %}
test -f "{{ path }}" || { echo artifact not created: {{ name }}={{ path }}; exit 1; }
  {% else %}
    {% for path_ in path -%}
test -f "{{ path_ }}" || { echo artifact not created: {{ name }}={{ path_ }}; exit 1; }
    {% endfor %}
  {% endif %}
{% endfor %}
{% endif %}
{% endblock %}
