{% extends "!job.sh" %}

{% block run %}

{% set jobids = job_blocking_status.keys()|list %}
{% set blocking_jobs = [] %}
{% set non_blocking_jobs = [] %}
{% for jobid, is_blocking in job_blocking_status.items() %}
  {% if is_blocking %}
    {% do blocking_jobs.append(jobid) %}
  {% else %}
    {% do non_blocking_jobs.append(jobid) %}
  {% endif %}
{% endfor %}
{% set num_blocking = blocking_jobs|length %}
{% set num_non_blocking = non_blocking_jobs|length %}
{% set num_total = jobids|length %}

CHECK_INTERVAL={{ check_interval|default(10) }}

echo "========== Woom Sentinel =========="
echo "Monitoring: {{ num_total }} jobs ({{ num_blocking }} blocking) | Interval: ${CHECK_INTERVAL}s"
echo ""

# Job ids
JOBIDS=({{ jobids|join(' ') }})

# Non-blocking jobs
NON_BLOCKING_JOBS=({{ non_blocking_jobs|join(' ') }})

# Blocking status dictionary
declare -A IS_BLOCKING
{% for jobid, is_blocking in job_blocking_status.items() %}
IS_BLOCKING[{{ jobid }}]={{ "1" if is_blocking else "0" }}
{% endfor %}

# Status files
declare -A STATUS_FILES
{% for jobid, path in status_files.items() %}
STATUS_FILES[{{ jobid }}]="{{ path }}"
{% endfor %}

# Set default status to PENDING
declare -A status
for j in "${JOBIDS[@]}"; do status[$j]="PENDING"; done

# Array sizes
NUM_BLOCKING={{ num_blocking }}
NUM_NON_BLOCKING={{ num_non_blocking }}
NUM_TOTAL={{ num_total }}


check_status() {
    local jobid="$1"

    # Priority 1: Check status file (authoritative if exists)
    if [ -f "${STATUS_FILES[$jobid]}" ]; then
        local exit_code=$(cat "${STATUS_FILES[$jobid]}")
        echo "from status file: $exit_code" >> job.out
        if [ "$exit_code" = "0" ]; then
            echo "SUCCESS"
        else
            echo "FAILED"
        fi
        return 0
    fi

    # Priority 2: Query scheduler
{% if host.scheduler == 'slurm' -%}
    if squeue -j "$jobid" -h &>/dev/null; then
        squeue -j "$jobid" -h -o "%T" 2>/dev/null | tr -d ' '
    else
        local status=$(sacct -j "$jobid" -n -X -o State 2>/dev/null | head -1 | tr -d ' ')
        [ -n "$status" ] && echo "$status" || echo "UNKNOWN"
    fi
{% elif host.scheduler == 'pbspro' -%}
    if qstat "$jobid" &>/dev/null; then
        qstat "$jobid" 2>/dev/null | tail -1 | awk '{print $5}'
        echo "from qstat file: " $(qstat "$jobid" 2>/dev/null | tail -1 | awk '{print $5}') >> job.out
    else
        local qstat_history=$(qstat -x -f "$jobid" 2>/dev/null)

        if [ -z "$qstat_history" ]; then
            echo "UNKNOWN"
            return 0
        fi

        local job_state=$(echo "$qstat_history" | grep "job_state" | cut -d= -f2 | tr -d ' ')
        echo "from qstat history file: $job_state" >> job.out
        if [ "$job_state" = "F" ]; then
            local exit_status=$(echo "$qstat_history" | grep "Exit_status" | cut -d= -f2 | tr -d ' ')
            [ "$exit_status" = "0" ] && echo "SUCCESS" || echo "FAILED"
            echo "from qstat history file with F: $exit_status" >> job.out
        else
            echo "$job_state"
        fi
    fi
{% endif %}
}

kill_all() {
    echo -e "\n\n========== FAILURE: $1 ==========" >&2
    echo "Killing all jobs..." >&2
    local n=0
{% if host.scheduler == 'slurm' -%}
    for j in "${JOBIDS[@]}"; do
        [ "$j" != "$SLURM_JOB_ID" ] && scancel "$j" 2>/dev/null && echo "  ✗ $j" >&2 && echo 1 > "${STATUS_FILES[$j]}" && n=$((n+1))
    done
{% elif host.scheduler == 'pbspro' -%}
    local my_id=$(echo $PBS_JOBID | cut -d. -f1)
    for j in "${JOBIDS[@]}"; do
        local jid=$(echo $j | cut -d. -f1)
        [ "$jid" != "$my_id" ] && qdel -W force "$j" 2>/dev/null && echo "  ✗ $j" >&2 && echo 1 > "${STATUS_FILES[$j]}" && n=$((n+1))
    done
{% endif %}
    echo "Killed $n jobs" >&2
    echo "===================================" >&2
    exit 1
}

terminate_non_blocking_jobs() {
    [ $NUM_NON_BLOCKING -eq 0 ] && return 0

    echo -e "\n==================================="
    echo "Terminating non-blocking jobs..."
    local n=0
{% if host.scheduler == 'slurm' -%}
    for j in "${NON_BLOCKING_JOBS[@]}"; do
        [ "${status[$j]}" = "SUCCESS" ] && continue

        if [ "$j" != "$SLURM_JOB_ID" ]; then
            # Mark job as intentionally terminating
            touch "${STATUS_FILES[$j]%.status}.terminating"
            scancel --signal=TERM "$j" 2>/dev/null && {
                echo "  ✓ $j"
                echo 0 > "${STATUS_FILES[$j]}"
                n=$((n+1))
            }
        fi
    done
{% elif host.scheduler == 'pbspro' -%}
    local my_id=$(echo $PBS_JOBID | cut -d. -f1)
    for j in "${NON_BLOCKING_JOBS[@]}"; do
        [ "${status[$j]}" = "SUCCESS" ] && continue

        local jid=$(echo $j | cut -d. -f1)
        if [ "$jid" != "$my_id" ]; then
            # Mark job as intentionally terminating
            touch "${STATUS_FILES[$j]%.status}.terminating"
            qdel "$j" 2>/dev/null && {
                echo "  ✓ $j"
                echo 0 > "${STATUS_FILES[$j]}"
                n=$((n+1))
            }
        fi
    done
{% endif %}
    echo "Terminated $n non-blocking jobs"
    echo "==================================="
}

echo "Monitoring started..."
rm -rf job.out

while true; do
    running=0 pending=0 blocking_done=0 total_done=0

    for j in "${JOBIDS[@]}"; do
        if [ "${status[$j]}" = "SUCCESS" ]; then
            total_done=$((total_done+1))
            [ "${IS_BLOCKING[$j]}" = "1" ] && blocking_done=$((blocking_done+1))
            continue
        fi

        s=$(check_status "$j")
        echo after check status $j $s

        case "$s" in
            SUCCESS)
                [ "${status[$j]}" != "SUCCESS" ] && echo "[$(date +'%H:%M:%S')] ✓ $j"
                status[$j]="SUCCESS"
                total_done=$((total_done+1))
                [ "${IS_BLOCKING[$j]}" = "1" ] && blocking_done=$((blocking_done+1))
                ;;
            FAILED{% if host.scheduler == 'slurm' %}|TIMEOUT|CANCELLED|NODE_FAIL|PREEMPTED|OUT_OF_MEMORY{% endif %})
                echo "[$(date +'%H:%M:%S')] ✗ $j ($s)"
                kill_all "$j"
                ;;
            {% if host.scheduler == 'slurm' -%}
            RUNNING|COMPLETING)
            {% elif host.scheduler == 'pbspro' -%}
            R)
            {% endif -%}
                [ "${status[$j]}" = "PENDING" ] && echo "[$(date +'%H:%M:%S')] → $j"
                status[$j]="RUNNING"
                running=$((running+1))
                ;;
            *)
                pending=$((pending+1))
                ;;
        esac
    done

    echo -ne "\r[$(date +'%H:%M:%S')] Blocking: $blocking_done/$NUM_BLOCKING | Total: $total_done/$NUM_TOTAL"
    [ $running -gt 0 ] && echo -ne " | $running run"
    [ $pending -gt 0 ] && echo -ne " | $pending pend"
    echo -ne "     "

    [ $blocking_done -eq $NUM_BLOCKING ] && {
        echo -e "\n\n========== SUCCESS: All blocking jobs completed =========="
        terminate_non_blocking_jobs
        exit 0
    }

    sleep $CHECK_INTERVAL
done
{% endblock %}

{% block post_run -%}
{% endblock %}
