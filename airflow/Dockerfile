ARG AIRFLOW_VERSION=3.0.6
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER airflow
ENV PIP_USER=false
RUN pip install --no-cache-dir \
    apache-airflow-providers-fab \
    pendulum \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt
