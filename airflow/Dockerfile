FROM apache/airflow:2.8.1-python3.11

# Pre-install pipeline dependencies so containers start fast
RUN pip install --no-cache-dir \
    minio==7.2.20 \
    pandas \
    psycopg2-binary \
    pandera
