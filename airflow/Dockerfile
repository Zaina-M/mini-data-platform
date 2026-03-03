FROM apache/airflow:2.8.1-python3.11

# Pre-install pipeline dependencies so containers start fast
RUN pip install --no-cache-dir \
    minio==7.2.20 \
    pandas \
    psycopg2-binary \
    pandera

# Create required directories for logging
USER root
RUN mkdir -p /opt/airflow/logs/scheduler /opt/airflow/logs/dag_processor_manager /opt/airflow/logs/pipeline && \
    chown -R airflow:root /opt/airflow/logs
USER airflow
