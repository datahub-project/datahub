# Copying MLflow Runs Between Servers

This guide explains how to use `copy_mlflow_run.py` to copy MLflow runs from your local server to the hosted AWS SageMaker MLflow tracking server.

## Prerequisites

1. Ensure your `.env` file (in `datahub-integrations-service` directory) has the correct AWS profile configured:

   ```bash
   AWS_PROFILE=acryl-read-write
   ```

2. Make sure your local MLflow server is running on `http://localhost:9090`

3. Make sure you're logged into AWS SSO:

   ```bash
   aws sso login --profile acryl-read-write
   ```

4. The script requires `mlflow`, `sagemaker-mlflow`, `boto3`, and `python-dotenv` packages (should already be installed in your venv)

## Usage

### Basic Usage

Copy a run by its name:

```bash
cd datahub-integrations-service/experiments/chatbot
python copy_mlflow_run.py ai_eval_wise-hare-663
```

Copy a run by its ID:

```bash
python copy_mlflow_run.py <32-character-run-id>
```

### Advanced Options

Specify a different experiment name:

```bash
python copy_mlflow_run.py ai_eval_wise-hare-663 --experiment "My Experiment"
```

Dry run (see what would be copied without actually copying):

```bash
python copy_mlflow_run.py ai_eval_wise-hare-663 --dry-run
```

## What Gets Copied

The script copies everything from the source run:

- ✓ All parameters
- ✓ All metrics (with full history, timestamps, and steps)
- ✓ All tags (except system-generated `mlflow.*` tags)
- ✓ All artifacts
- ✓ Run status and timestamps
- ✓ Experiment metadata

## Server Configuration

The script uses hardcoded server URLs:

- **Local:** `http://localhost:9090`
- **Remote:** `arn:aws:sagemaker:us-west-2:795586375822:mlflow-tracking-server/prod-mlflow-tracking-server-01`

The remote server requires:

- AWS SigV4 authentication (uses your AWS profile)
- S3 server-side encryption (AES256)

## Viewing Results

After copying, view the runs on the remote server using:

```bash
./open_mlflow.sh
```

## Troubleshooting

### AWS Authentication Issues

Make sure you're logged into AWS SSO:

```bash
aws sso login --profile acryl-read-write
```

### Local MLflow Server Not Running

Start the local server:

```bash
python3 -m mlflow server --host 127.0.0.1 --port 9090
```

### Run Not Found

The script searches by run name using the tag `mlflow.runName`. If not found, try using the run ID directly (you can find it in the MLflow UI URL).

## Examples

```bash
# Copy a specific AI eval run
../../venv/bin/python copy_mlflow_run.py ai_eval_wise-hare-663

# Preview what would be copied
../../venv/bin/python copy_mlflow_run.py ai_eval_wise-hare-663 --dry-run

# Copy from a different experiment
../../venv/bin/python copy_mlflow_run.py my-run-name --experiment "Custom Experiment"
```

## Example Output

```
Connecting to local MLflow server: http://localhost:9090

Searching for run by name: ai_eval_wise-hare-663
✓ Found run ID: 07d8aadcb5094865a8aee6a980984e71

Source Run Details:
  Name: ai_eval_wise-hare-663
  Experiment: Chatbot
  Status: FINISHED
  Parameters: 5
  Metrics: 32
  Tags: 7

Connecting to remote MLflow server: arn:aws:sagemaker:us-west-2:795586375822:mlflow-tracking-server/prod-mlflow-tracking-server-01
Using AWS profile: acryl-read-write
✓ Destination experiment ID: 1

Copying run: ai_eval_wise-hare-663
  Source run ID: 07d8aadcb5094865a8aee6a980984e71
  Status: FINISHED
  Destination run ID: 250252a8170e4c9da620c626c18a5df6
  Copying 5 parameters...
  Copying 32 metrics...
  Copying 7 tags...
  Copying artifacts...
    Downloading artifacts to /tmp/...
    Uploading artifacts...
    ✓ Artifacts copied
  ✓ Run copied successfully

✓ Successfully copied run!
  Destination run ID: 250252a8170e4c9da620c626c18a5df6

To view the run on remote server, use: ./open_mlflow.sh
```
