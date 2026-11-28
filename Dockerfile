FROM apache/airflow:3.1.0

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt

# dbt di venv terpisah: dependency dbt-core sering bentrok dengan constraint Airflow.
# Versi duckdb harus sama dengan yang dipakai loader (format file .duckdb).
USER root
RUN python -m venv /opt/dbt_venv && chown -R airflow:0 /opt/dbt_venv
USER airflow
RUN /opt/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.7 dbt-duckdb==1.11.1 duckdb==2.0.0
