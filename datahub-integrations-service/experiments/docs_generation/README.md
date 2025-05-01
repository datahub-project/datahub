# Docs generation Experimentation

## Prerequisites

1. Build and activate datahub-integrations-service python venv
2. Create `.env` file in `docs_generation` folder containing bedrock credentials
   ```.env
   BEDROCK_AWS_ACCESS_KEY_ID=
   BEDROCK_AWS_SECRET_ACCESS_KEY=
   BEDROCK_AWS_REGION=
   ```
3. Start local mlflow server
   `python3 -m mlflow server --host 127.0.0.1 --port 9090`
4. Configure mlflow tracking uri in above .env file
   ```.env
   MLFLOW_TRACKING_URI="http://localhost:9090"
   ```
5. Download `eval_urns.json` from [here](https://www.notion.so/acryldata/Scale-Documentation-Generation-to-More-Than-100-Columns-1cafc6a642778077ad9cde096a5d6362?pvs=4#1d6fc6a6427780be9ee3fd6d658d0527) to docs_generation folder
6. Generate `graph_credentials.json` file using [this script](https://github.com/acryldata/experimental/blob/main/hsheth/bulk-graph-creds/generate_many_graph_credentials.py) and copy to `docs_generation` folder
7. Run `python3 generate_eval_data.py` to download eval data locally.

## Running prompt engineering experiment run to generate descriptions

Modify <CURRENT_PROMPT> in `run_prompt_experiment.py` and run `python3 run_prompt_experiment.py` to generate descriptions.
An mlflow experiment run will be logged.

## Running AI evaluation on existing prompt engineering experiment run

Run `python3 run_ai_annotations.py <RUN_NAME>` to generate ai evaluations a prompt experiment run with name `RUN_NAME`
An mlflow experiment run will be logged with prefix 'ai_annotations\*' to referenced run's name.

## Adding human annotations on existing prompt engineering experiment run

Run `streamlit run add_human_annotations_ui.py -- --run-name=<RUN_NAME>` and add annotations on UI on browser then click **'Submit and Finish`** to save human annotations.
If referenced `RUN_NAME` has earlier human annotations then those will be prefilled in annotations UI OR
If referenced `RUN_NAME` has ai annotations then those will be prefilled in annotations UI.
An mlflow experiment run will be logged with prefix 'human_annotations\*' to referenced run's name.

## Importing existing mlflow prompt experiment

1. If you have eval_results_table.json file of existing mlflow prompt experiment, simply run
   `python3 import_human_eval <eval_file_path> --run_name <run_name_to_import_as>`
