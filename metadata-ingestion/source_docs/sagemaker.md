# SageMaker

## Setup

To install this plugin, run `pip install 'acryl-datahub[sagemaker]'`.

## Capabilities

This plugin extracts the following:

- Feature groups
- Models, jobs, and lineage between the two (e.g. when jobs output a model or a model is used by a job)

## Quickstart recipe

Use the below recipe to get started with ingestion. See [below](#config-details) for full configuration options.

```yml
source:
  type: sagemaker
  config:
    aws_region: # aws_region_name, i.e. "eu-west-1"
    env: # environment for the DatasetSnapshot URN, one of "DEV", "EI", "PROD" or "CORP". Defaults to "PROD".
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                 | Required | Default | Description |
| ------------------------------------- | -------- | ------- | ----------- |
| `aws_region`                          |          |         |             |
| `env`                                 |          |         |             |
| `aws_access_key_id`                   |          |         |             |
| `aws_secret_access_key`               |          |         |             |
| `aws_session_token`                   |          |         |             |
| `aws_role`                            |          |         |             |
| `extract_feature_groups`              |          |         |             |
| `extract_models`                      |          |         |             |
| `extract_jobs.auto_ml`                |          |         |             |
| `extract_jobs.compilation`            |          |         |             |
| `extract_jobs.edge_packaging`         |          |         |             |
| `extract_jobs.hyper_parameter_tuning` |          |         |             |
| `extract_jobs.labeling`               |          |         |             |
| `extract_jobs.processing`             |          |         |             |
| `extract_jobs.training`               |          |         |             |
| `extract_jobs.transform`              |          |         |             |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
