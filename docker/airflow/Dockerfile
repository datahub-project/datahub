FROM apache/airflow:2.3.1
ARG RELEASE_VERSION
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  vim gcc libc-dev libssl-dev libffi-dev librdkafka-dev pkg-config g++\
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && sudo -u airflow pip install --upgrade pip setuptools wheel \
  && sudo -u airflow pip install --user --no-cache-dir \
  "acryl_datahub[airflow, bigquery, mysql, snowflake, looker, lookml, datahub-rest, datahub-kafka]==$RELEASE_VERSION" \
  'apache-airflow-providers-snowflake' \
  && apt-get remove --purge -yqq gcc libc-dev libssl-dev libffi-dev pkg-config g++
USER airflow
