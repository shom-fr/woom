#!/bin/bash

# Prolog
{{ task.export_prolog() }}

# Set the environment
{{ task.export_env() }}

# Go to run dir
{{ task.export_run_dir() }}

# Run the commandline(s)
{{ task.export_commandline() }}

# Epilog
{{ task.export_epilog() }}
