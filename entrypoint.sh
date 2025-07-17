#!/bin/bash

# Run a command in the background.
_evalBg() {
    eval "$@" &>/dev/null & disown;
}

# Run dbt deps
dbt deps --project-dir /usr/app/respiratorios/dbt/

# Start dagster-daemon in background
cmd="dagster-daemon run";
_evalBg "${cmd}";

# Start dagster-webserver
dagster-webserver -h 0.0.0.0 -p 3000 --path-prefix ${DAGSTER_PATH_PREFIX}