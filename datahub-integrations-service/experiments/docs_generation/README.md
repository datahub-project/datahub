# Docs Generation Experimentation

## Quick Start - Testing Documentation Generation with Extra Instructions

### Simple Test Script

Edit `test_generation_simple.py` with your URN and run:

```bash
# Edit the TEST_URN variable in the script, then:
python test_generation_simple.py
```

### Configuration (Edit in Script)

```python
# Configuration - EDIT THESE FOR YOUR TEST
TEST_URN = "urn:li:dataset:(urn:li:dataPlatform:databricks,dbc-a2674c21-c551.acryl_metastore.main.information_schema.catalog_privileges,PROD)"
MOCK_INSTRUCTIONS = "Always mention data quality and compliance considerations. Use business-friendly language."
USE_MOCK_INSTRUCTIONS = False  # Set to True to use mock instructions, False to fetch from GraphQL
```

### Features

- **Quick Testing**: Hardcoded URN for rapid iteration
- **Flexible Instructions**: Toggle between mock instructions and real GraphQL fetch
- **Clear Output**: Shows table description, column descriptions, and timing
- **AWS Configuration**: Automatically loads from `.env` file
- **Entity Statistics**: Displays metadata about the dataset

### Example Output

```
================================================================================
DOCUMENTATION GENERATION TEST
================================================================================

AWS Configuration:
  AWS_PROFILE: acryl-read-write
  AWS_REGION: us-west-2
  BEDROCK_MODEL: cohere.embed-english-v3

✓ Connected to DataHub

----------------------------------------
TESTING EXTRA INSTRUCTIONS
----------------------------------------
Using mock instructions: Always mention data quality and compliance conside...

----------------------------------------
GENERATING DOCUMENTATION FOR: urn:li:dataset:(urn:li:dataPlatform:databricks,...)
----------------------------------------
✓ Generation completed in 7.82s

----------------------------------------
RESULTS
----------------------------------------

✓ Table Description (1492 chars):
--------------------
### catalog_privileges

The `catalog_privileges` table in the DataHub system provides information about...

✓ Column Descriptions (6 columns):
--------------------
  1. grantor: The user or role that granted the privilege.
  2. grantee: The user or role that received the granted privilege.
  ... and 4 more columns

📊 Entity Statistics:
  - URN: urn:li:dataset:(urn:li:dataPlatform:databricks,...)
  - Total columns: 6
  - Table name: catalog_privileges
```

## Extra Instructions Feature

The documentation generation now supports custom instructions that can:

1. Be fetched from GraphQL (`globalSettings.documentationAi.instructions`)
2. Be overridden for testing
3. Be cached for 5 minutes to reduce API calls

Example custom instructions:

```
"Always mention data quality implications and compliance considerations.
Use business-friendly language and avoid technical jargon.
Focus on the business value and use cases of the data."
```

## Prerequisites for Full Experimentation

1. Build and activate datahub-integrations-service python venv
2. Create `.env` file in `datahub-integrations-service` folder containing bedrock credentials
   ```.env
   AWS_PROFILE=<profile corresponding to your Acryl_Developer_Engineer role>
   DATAHUB_INTEGRATIONS_SEND_TELEMETRY_EVENTS=false
   ```
3. Start local mlflow server
   `python3 -m mlflow server --host 127.0.0.1 --port 9090`
4. Configure mlflow tracking uri in above .env file
   ```.env
   MLFLOW_TRACKING_URI="http://localhost:9090"
   ```
   Also export MLFLOW_TRACKING_URI="http://localhost:9090" in terminal where python commands will be run.
5. Download `eval_set.json` and `eval_set_guidelines.yaml` and `deployment_details.json` from [here](https://www.notion.so/acryldata/Scale-Documentation-Generation-to-More-Than-100-Columns-1cafc6a642778077ad9cde096a5d6362?pvs=4#1e4fc6a6427780ca9839e58416f3df8f) to `docs_generation/eval_config` folder
6. Generate `graph_credentials.json` file using [this script](https://github.com/acryldata/experimental/blob/main/hsheth/bulk-graph-creds/generate_many_graph_credentials.py) and copy to `datahub-integrations-service/experiments` folder
7. Run `python3 generate_eval_data.py` to download eval data locally.

### Setup to use google Vertex AI models instead of AWS Bedrock

_Pre-requisite_

- Request access to acryl-poc google project.
- Install gcloud cli and follow one-time setup as shown below

```sh
$ gcloud auth application-default login
$ gcloud config set project acryl-poc
```

Environment variables to be set via `.env` created above

```.env
VERTEXAI_PROJECT="acryl-poc"
VERTEXAI_LOCATION="us-east4"  # or whichever region you're using

DESCRIPTION_GENERATION_MODEL="google_vertexai/gemini-2.5-flash"
QUERY_DESCRIPTION_GENERATION_MODEL="google_vertexai/gemini-2.5-flash"
```

## Running prompt engineering experiment run to generate descriptions

Modify <CURRENT_PROMPT> in `run_prompt_experiment.py` and run `python3 run_prompt_experiment.py` to generate descriptions.
An mlflow experiment run will be logged.

## Running AI evaluation on existing prompt engineering experiment run

Run `python3 run_ai_annotations.py <RUN_NAME>` to generate ai evaluations a prompt experiment run with name `RUN_NAME`
An mlflow experiment run will be logged with prefix 'ai_annotations\*' to referenced run's name.

## Adding human annotations on existing prompt engineering experiment run

**Prerequisite**: Run AI evaluation on this run first
Run `streamlit run add_human_annotations_ui -- --run-name=<RUN_NAME>` and add annotations on UI on browser then click **'Submit and Finish`** to save human annotations.
If referenced `RUN_NAME` has earlier human annotations then those will be prefilled in annotations UI OR
If referenced `RUN_NAME` has ai annotations then those will be prefilled in annotations UI.
An mlflow experiment run will be logged with prefix 'human_annotations\*' to referenced run's name.
In addition, `eval_config/eval_set_guidelines.yaml` file will also be updated when individual table eval and guidelines are submitted.

## Comparing AI evaluation with human evaluation

Once both AI evaluation and human annotations are done and submitted to mlflow
Run `python3 eval_ai_judge.py <RUN_NAME>` to compute AI judge accuracy
An mlflow experiment run will be logged with prefix 'ai_annotations\*' and tag `evaluation_type: ai_judge_eval`,
which will contain artifact `ai_judge_accuracy.json` that captures overall metric wise comparison.

## Importing existing mlflow prompt experiment

1. If you have eval_results_table.json file of existing mlflow prompt experiment, simply run
   `python3 import_human_eval <eval_file_path> --run_name <run_name_to_import_as>`

## Configuration

- **Metrics**: See `eval_config/metrics_config.yaml` for metrics configuration
- **Environment Variables**:
  - `DESCRIPTION_GENERATION_MODEL`: Model for table/column descriptions
  - `QUERY_DESCRIPTION_GENERATION_MODEL`: Model for query descriptions

## Troubleshooting

### AWS Credentials Issues

```bash
# Ensure AWS credentials are configured:
cat ~/.aws/credentials  # Check for profile
# Or set AWS_PROFILE in .env file
```

### Debugging Tips

1. Start with `USE_MOCK_INSTRUCTIONS = True` to test without GraphQL
2. Check AWS configuration in the script output
3. Review generated descriptions to verify instruction adherence
4. Monitor the timing to identify performance issues
5. Check the generated documentation in the DataHub UI after running
