# Openlineage Converter

## Overview

It converts arbitrary Openlineage events to DataHub Aspects.

## Known Issues

- Currently, it was tested only with Spark, Airflow and Trino events.
- Due to Openlineage's stateless nature, it is possible not all the inputs or outputs captured.
