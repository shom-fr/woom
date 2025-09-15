#!/bin/bash

# Prolog
set -eo pipefail
on_exit() {
    status=$?
    echo $status > "$WOOM_SUBMISSION_DIR/job.status"
}
trap on_exit EXIT

# Set the environment
{{ task.export_env(params) }}

# Go to run dir
{{ task.export_run_dir() }}

# Run the commandline(s)
{{ task.export_commandline() }}
