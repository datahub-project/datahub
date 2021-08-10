# SageMaker

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[sagemaker]'`.

## Capabilities

This plugin extracts the following:

- Feature groups
- Models, jobs, and lineage between the two (e.g. when jobs output a model or a model is used by a job)

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: sagemaker
  config:
    # Coordinates
    aws_region: "my-aws-region"

sink:
  # sink configs
```

## Config details

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                                 | Required | Default      | Description                                                                        |
| ------------------------------------- | -------- | ------------ | ---------------------------------------------------------------------------------- |
| `aws_region`                          | ✅       |              | AWS region code.                                                                   |
| `env`                                 |          | `"PROD"`     | Environment to use in namespace when constructing URNs.                            |
| `aws_access_key_id`                   |          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html |
| `aws_secret_access_key`               |          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html |
| `aws_session_token`                   |          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html |
| `aws_role`                            |          | Autodetected | See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html |
| `extract_feature_groups`              |          | `True`       | Whether to extract feature groups.                                                 |
| `extract_models`                      |          | `True`       | Whether to extract models.                                                         |
| `extract_jobs.auto_ml`                |          | `True`       | Whether to extract AutoML jobs.                                                    |
| `extract_jobs.compilation`            |          | `True`       | Whether to extract compilation jobs.                                               |
| `extract_jobs.edge_packaging`         |          | `True`       | Whether to extract edge packaging jobs.                                            |
| `extract_jobs.hyper_parameter_tuning` |          | `True`       | Whether to extract hyperparameter tuning jobs.                                     |
| `extract_jobs.labeling`               |          | `True`       | Whether to extract labeling jobs.                                                  |
| `extract_jobs.processing`             |          | `True`       | Whether to extract processing jobs.                                                |
| `extract_jobs.training`               |          | `True`       | Whether to extract training jobs.                                                  |
| `extract_jobs.transform`              |          | `True`       | Whether to extract transform jobs.                                                 |

## Compatibility

Coming soon!

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/)!
