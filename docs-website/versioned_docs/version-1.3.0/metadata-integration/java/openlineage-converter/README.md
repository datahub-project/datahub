---
title: Openlineage Converter
slug: /metadata-integration/java/openlineage-converter
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/metadata-integration/java/openlineage-converter/README.md
---
# Openlineage Converter

## Overview

It converts arbitary Openlineage events to a DataHub Aspects.

## Known Issues

- Currently, it was tested only with Spark and Airflow events.
- Due to Openlineage's stateless nature, it is possible not all the inputs or outputs captured.
