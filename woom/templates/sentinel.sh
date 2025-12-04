{% extends "!job.sh" %}

{% block run %}

CHECK_INTERVAL={{ check_interval|default(10) }}

echo "========== Woom Sentinel =========="
echo "Monitoring: {{ jobids|length }} jobs | Interval: ${CHECK_INTERVAL}s"
echo ""


JOBIDS=({{ jobids|join(' ') }})

# Set default status to PENDING
declare -A status
for j in "${JOBIDS[@]}"; do status[$j]="PENDING"; done

# Set status files
declare -A STATUS_FILES
{% for jobid, path in status_files.items() %}
STATUS_FILES[{{ jobid }}]="{{ path }}"
{% endfor %}

check_status() {

    # From status file
    if test -f ${STATUS_FILES[$1]}; then
        if [ "$(cat ${STATUS_FILES[$1]})" = "0" ] ; then
            echo "COMPLETED"
        else
            echo "FAILED"
        fi
    else

    # From scheduler
{% if host.scheduler == 'slurm' -%}
        squeue -j $1 -h &> /dev/null && squeue -j $1 -h -o "%T" 2>/dev/null | tr -d ' ' || sacct -j $1 -n -X -o State 2>/dev/null | head -1 | tr -d ' '

{% elif host.scheduler == 'pbspro' -%}
        if qstat $1 &> /dev/null; then
            qstat $1 2>/dev/null | tail -1 | awk '{print $5}'
        else
            local e=$(qstat -x -f $1 2>/dev/null | grep Exit_status | cut -d= -f2 | tr -d ' ')
            local s=$(qstat -x -f $1 2>/dev/null | grep job_state | cut -d= -f2 | tr -d ' ')
            [ "$s" = "F" ] && { [ "$e" = "0" ] && echo "COMPLETED" || echo "FAILED"; }
        fi
{% endif %}
    fi
}

kill_all() {
    echo -e "\n\n========== FAILURE: $1 ==========" >&2
    echo "Killing all jobs..." >&2
    local n=0
{% if host.scheduler == 'slurm' -%}
    for j in "${JOBIDS[@]}"; do
        [ "$j" != "$SLURM_JOB_ID" ] && scancel $j 2>/dev/null && echo "  ✓ $j" >&2 && n=$((n+1))
    done
{% elif host.scheduler == 'pbspro' -%}
    local my_id=$(echo $PBS_JOBID | cut -d. -f1)
    for j in "${JOBIDS[@]}"; do
        local jid=$(echo $j | cut -d. -f1)
        [ "$jid" != "$my_id" ] && qdel $j 2>/dev/null && echo "  ✓ $j" >&2 && n=$((n+1))
    done
{% endif %}
    echo "Killed $n jobs" >&2
    echo "===================================" >&2
    exit 1
}

echo "Monitoring started..."

while true; do
    running=0 pending=0 done=0

    for j in "${JOBIDS[@]}"; do
        [ "${status[$j]}" = "COMPLETED" ] && { done=$((done+1)); continue; }

        s=$(check_status $j)

        case "$s" in
            COMPLETED)
                [ "${status[$j]}" != "COMPLETED" ] && echo "[$(date +'%H:%M:%S')] ✓ $j" && done=$((done+1))
                status[$j]="COMPLETED"
                ;;
            FAILED{% if host.scheduler == 'slurm' %}|TIMEOUT|CANCELLED|NODE_FAIL|PREEMPTED|OUT_OF_MEMORY{% endif %})
                echo "[$(date +'%H:%M:%S')] ✗ $j ($s)"
                kill_all $j
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

    echo -ne "\r[$(date +'%H:%M:%S')] $done/{{ jobids|length }} done"
    [ $running -gt 0 ] && echo -ne ", $running run"
    [ $pending -gt 0 ] && echo -ne ", $pending pend"
    echo -ne "     "

    [ $done -eq {{ jobids|length }} ] && {
        echo -e "\n\n========== SUCCESS: All jobs completed =========="
        exit 0
    }

    sleep $CHECK_INTERVAL
done
{% endblock %}

{% block post_run -%}
{% endblock %}
