# Python ETL examples

ETL scripts written in Python.

## Prerequisites

1. Before running any python metadata ingestion job, you should make sure that DataHub backend services are all running.
The easiest way to do that is through [Docker images](../../docker).
2. You also need to build the `mxe-schemas` module as below.
   ```
   ./gradlew :metadata-events:mxe-schemas:build
   ```
   This is needed to generate `MetadataChangeEvent.avsc` which is the schema for `MetadataChangeEvent` Kafka topic.
3. All the scripts are written using Python 3 and most likely won't work with Python 2.x interpreters.
   You can verify the version of your Python using the following command.
   ```
   python --version
   ```
   We recommend using [pyenv](https://github.com/pyenv/pyenv) to install and manage your Python environment.
4. Before launching each ETL ingestion pipeline, you can install/verify the library versions as below.
   ```
   pip install --user -r requirements.txt
   ```