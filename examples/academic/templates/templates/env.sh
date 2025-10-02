{% extends "!env.sh" %}

{% block env_vars -%}
{{ super() }}
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-4}
{% endblock %}

{% block custom %}
# Utilities
function log_message() {
    echo "[$(date +%Y-%m-%d_%H:%M:%S)] $*" | \
        tee -a ${WOOM_LOG_DIR}/custom.log
}

# Initialization
log_message "Environment is initialized for ${WOOM_TASK_NAME}"
{% endblock %}
