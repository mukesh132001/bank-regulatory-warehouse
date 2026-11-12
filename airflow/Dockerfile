# Airflow image for the Bank Regulatory Reporting Warehouse.
# Built by docker-compose (build context = repo root).

ARG AIRFLOW_IMAGE=apache/airflow:3.3.2-python3.12
FROM ${AIRFLOW_IMAGE}

# PySpark needs a Java runtime (Spark 4 supports Java 17 and 21).
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# The pipeline's own dependencies (dbt, Faker, psycopg2, boto3, PySpark) live in
# a separate virtualenv, so they can never conflict with Airflow's pinned packages.
# DAG tasks activate this venv before running a step.
COPY requirements.txt /tmp/project-requirements.txt
RUN python -m venv /home/airflow/project-venv \
 && PIP_USER=false /home/airflow/project-venv/bin/pip install --no-cache-dir \
      -r /tmp/project-requirements.txt

ENV PROJECT_VENV=/home/airflow/project-venv
