{% block header -%}
#!/bin/bash

# Prolog
set -eo pipefail
on_exit() {
    status=$?
    echo $status > "$WOOM_SUBMISSION_DIR/job.status"
    exit $status
}
trap on_exit EXIT
{% endblock %}

{% block env -%}
{{ task.export_env(params) }}
{% endblock %}

{% block pre_run -%}
# Go to run dir
{{ task.export_run_dir() }}
{% endblock %}

{% block run -%}
# Run the commandline(s)
{{ task.export_commandline() }}
{% endblock %}

{% block post_run -%}
{% if task.artifacts %}
# Check artifacts
{{ task.export_artifacts_checking() }}
{% endif %}
{% endblock %}
