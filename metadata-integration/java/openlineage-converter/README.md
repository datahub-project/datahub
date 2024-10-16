# Openlineage Converter

## Overview
It converts arbitary Openlineage events to a DataHub Aspects.

## Known Issues
- Currently, it was tested only with Spark and Airflow events.
- Due to Openlineage's stateless nature, it is possible not all the inputs or outputs captured.
