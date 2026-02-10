FROM apache/airflow:3.1.6

# Switch to root to copy files and set permissions
USER root

# Copy DAGs and plugins into the image
COPY dynamic_dags/dags/ /opt/airflow/dags/
# Also copy plugins to the standard Airflow plugins location for plugin discovery
COPY dynamic_dags/plugins/ /opt/airflow/plugins/

# Set proper permissions
RUN chown -R airflow:root /opt/airflow/dags /opt/airflow/plugins && \
    chmod -R 755 /opt/airflow/dags /opt/airflow/plugins

# Ensure /opt/airflow/dags is in PYTHONPATH so powerx_dag_common can be imported
# Also add /opt/airflow so pyscripts module can be imported
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags:/opt/airflow"

# Switch back to airflow user
USER airflow
