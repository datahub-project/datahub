FROM apache/airflow:2.1.3-python3.9
ARG RELEASE_VERSION
RUN pip install \
  "acryl_datahub[airflow, bigquery, mysql, snowflake, looker, lookml, datahub-rest, datahub-kafka]==$RELEASE_VERSION" \
  'pyarrow~=3.0.0' \
  'apache-airflow-providers-snowflake' \
  'pandas'

