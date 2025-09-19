# Chatbot Experimentation

## Prerequisites

1. Run `../gradlew installDev` to install dependencies + setup the venv
1. Create `.env` file in `datahub-integrations-service` folder containing bedrock credentials

   ```bash
   AWS_PROFILE=<profile corresponding to your Acryl_Developer_Prod role>
   DATAHUB_TELEMETRY_ENABLED=false
   MLFLOW_TRACKING_URI=arn:aws:sagemaker:us-west-2:795586375822:mlflow-tracking-server/prod-mlflow-tracking-server-01
   MLFLOW_TRACKING_AWS_SIGV4=true
   MLFLOW_S3_UPLOAD_EXTRA_ARGS={"ServerSideEncryption": "AES256"}
   DATAHUB_INTEGRATIONS_SEND_TELEMETRY_EVENTS=false
   ```

   > **Note:** If you would like to use a local MLFlow server:
   > Start local mlflow server using `python3 -m mlflow server --host 127.0.0.1 --port 9090`
   > Use the following .env file:

   ```bash
   AWS_PROFILE=<profile corresponding to your Acryl_Developer_Prod role>
   DATAHUB_TELEMETRY_ENABLED=false
   MLFLOW_TRACKING_URI="http://localhost:9090"
   DATAHUB_INTEGRATIONS_SEND_TELEMETRY_EVENTS=false
   ```

1. Tip: Use `direnv` to automatically load the `.env` file when working in the `datahub-integrations-service` folder
   ```bash
   # .envrc
   source venv/bin/activate
   unset PS1
   dotenv
   ```
1. Download `prompts.yaml` (stored in Notion) into `datahub-integrations-service/experiments/chatbot` folder
1. Generate `graph_credentials.json` file using [this script](https://github.com/acryldata/experimental/blob/main/hsheth/graph_credentials/generate_many_graph_credentials.py) and copy to `datahub-integrations-service/experiments` folder
   ```bash
   cd experimental/hsheth/graph_credentials
   python3 generate_many_graph_credentials.py  # you might need to adjust `DATAHUB_APPS_DIR`
   cp graph_credentials.json <path to datahub>/datahub-integrations-service/experiments
   ```

## Evals

We have a bunch of test cases / prompts, tied to specific customer instances, alongside guidelines for what we expect the response to include.

To run the bot + evals:

1. `run.py` runs the chatbot on a set of prompts and saves the results to mlflow / local files.
2. `run_ai_eval.py` runs AI evaluation on a completed chatbot experiment using MLflow evaluate.
3. `chat_review.py` allows you to review the results of the chatbot + runs LLM judge evals.

Evals logged to our shared MLFlow instance can be viewed via the MLFlow web UI. It requires a pre-signed URL so use the following convenience script to open a browser window:

```
./open_mlflow.sh
```

### Eval test case format

`prompts.yaml` contains a list of the prompts to run. Format is a list of dicts, where each entry has the following fields:

- `id`: a unique identifier for the prompt
- `instance`: the instance of the chatbot to run the prompt on
- `message`: the prompt message
- `response_guidelines` (Optional): the response guidelines for the prompt; passed to the LLM judge
- `tags` (Optional): list of tags
- `expected_tool_calls` (Optional): the list of tool calls supposed to be present in chat history

## Other tools

- `chat_ui.py` is for running interactive chat sessions without needing to set up Slack. Run using `streamlit run experiments/chatbot/chat_ui.py`.
