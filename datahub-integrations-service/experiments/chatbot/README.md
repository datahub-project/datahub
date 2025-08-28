## Prerequisites

1. Build and activate datahub-integrations-service python venv
2. Create `.env` file in `datahub-integrations-service` folder containing bedrock credentials
   ```.env
   BEDROCK_AWS_ACCESS_KEY_ID=
   BEDROCK_AWS_SECRET_ACCESS_KEY=
   BEDROCK_AWS_REGION=
   DATAHUB_TELEMETRY_ENABLED=false
   ```
3. Start local mlflow server
   `python3 -m mlflow server --host 127.0.0.1 --port 9090`
4. Configure mlflow tracking uri in above .env file
   ```.env
   MLFLOW_TRACKING_URI="http://localhost:9090"
   ```
   Also export MLFLOW_TRACKING_URI="http://localhost:9090" in terminal where python commands will be run.
5. Copy `prompts.yaml` in `datahub-integrations-service/experiments/chatbot` folder
6. Generate `graph_credentials.json` file using [this script](https://github.com/acryldata/experimental/blob/main/hsheth/bulk-graph-creds/generate_many_graph_credentials.py) and copy to `datahub-integrations-service/experiments` folder.

# Chatbot evals

We have a bunch of test cases / prompts, tied to specific customer instances, alongside guidelines for what we expect the response to include.

To run the bot + evals:

1. `run.py` runs the chatbot on a set of prompts and saves the results to mlflow / local files.
2. `run_ai_eval.py` runs AI evaluation on a completed chatbot experiment using MLflow evaluate.
3. `chat_review.py` allows you to review the results of the chatbot + runs LLM judge evals.

## Eval test case format

`prompts.yaml` contains a list of the prompts to run. Format is a list of dicts, where each entry has the following fields:

- `id`: a unique identifier for the prompt
- `instance`: the instance of the chatbot to run the prompt on
- `message`: the prompt message
- `response_guidelines` (Optional): the response guidelines for the prompt; passed to the LLM judge
- `tags` (Optional): list of tags
- `expected_tool_calls` (Optional): the list of tool calls supposed to be present in chat history

## Other tools

- `chat_ui.py` is for running interactive chat sessions without needing to set up Slack.
