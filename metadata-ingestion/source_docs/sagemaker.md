# AWS SageMaker

To install this plugin, run `pip install 'acryl-datahub[sagemaker]'`.

Extracts:

- Feature groups
- Models, jobs, and lineage between the two (e.g. when jobs output a model or a model is used by a job)

```yml
source:
  type: sagemaker
  config:
    aws_region: # aws_region_name, i.e. "eu-west-1"
    env: # environment for the DatasetSnapshot URN, one of "DEV", "EI", "PROD" or "CORP". Defaults to "PROD".

    # Credentials. If not specified here, these are picked up according to boto3 rules.
    # (see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
    aws_access_key_id: # Optional.
    aws_secret_access_key: # Optional.
    aws_session_token: # Optional.
    aws_role: # Optional (Role chaining supported by using a sorted list).

    extract_feature_groups: True # if feature groups should be ingested, default True
    extract_models: True # if models should be ingested, default True
    extract_jobs: # if jobs should be ingested, default True for all
      auto_ml: True
      compilation: True
      edge_packaging: True
      hyper_parameter_tuning: True
      labeling: True
      processing: True
      training: True
      transform: True
```
