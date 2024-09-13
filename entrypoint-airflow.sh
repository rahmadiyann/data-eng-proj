#!/bin/bash

# Set correct permissions
chown -R airflow:root /opt/spark/data/*
chmod -R 777 /opt/spark/data/*

# Run the original Airflow entrypoint (or webserver/scheduler as needed)
if [ "$1" = "webserver" ]; then
    exec airflow webserver
elif [ "$1" = "scheduler" ]; then
    # Migrate the database and create an admin user if necessary
    airflow db upgrade  # Use `upgrade` instead of `migrate` for database schema updates
    airflow users create --username admin --firstname Yusuf --lastname Ganiyu --role Admin --email airscholar@gmail.com --password admin
    exec airflow scheduler
else
    exec "$@"
fi