#!/bin/bash
set -e

# Setup Local Airflow Environment
export AIRFLOW_HOME="$(pwd)/airflow_local"
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

echo "============================================================"
echo "Setting up Local Airflow in: $AIRFLOW_HOME"
echo "============================================================"

mkdir -p $AIRFLOW_HOME

# Initialize Database
echo "[INFO] Running Airflow DB Migration..."
poetry run airflow db migrate

# Create Admin User
echo "[INFO] Creating Admin User..."
poetry run airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "============================================================"
echo "Setup Complete!"
echo "============================================================"
echo ""
echo "To start Airflow, run these commands in separate terminals:"
echo ""
echo "1. Webserver:"
echo "   export AIRFLOW_HOME=\"$(pwd)/airflow_local\""
echo "   poetry run airflow webserver -p 8080"
echo ""
echo "2. Scheduler:"
echo "   export AIRFLOW_HOME=\"$(pwd)/airflow_local\""
echo "   export JAVA_HOME=\"$JAVA_HOME\""
echo "   poetry run airflow scheduler"
echo ""
