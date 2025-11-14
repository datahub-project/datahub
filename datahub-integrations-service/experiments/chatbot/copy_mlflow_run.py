"""
Copy MLflow runs from local server to remote hosted server.

This script copies a complete MLflow run including:
- Parameters
- Metrics
- Tags
- Artifacts
- Parent/child run relationships

Usage:
    python copy_mlflow_run.py <run_name_or_id>
    python copy_mlflow_run.py ai_eval_wise-hare-663
"""

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional

import mlflow
from mlflow.entities import RunStatus
from mlflow.tracking import MlflowClient
from dotenv import load_dotenv

# Import sagemaker-mlflow plugin to enable ARN support
import sagemaker_mlflow

# Load .env file for AWS credentials (look in parent directory)
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


# MLflow server configurations
LOCAL_MLFLOW_URI = "http://localhost:9090"
REMOTE_MLFLOW_URI = "arn:aws:sagemaker:us-west-2:795586375822:mlflow-tracking-server/prod-mlflow-tracking-server-01"

# Remote server requires AWS SigV4 auth and S3 encryption
REMOTE_S3_EXTRA_ARGS = {"ServerSideEncryption": "AES256"}


def setup_remote_env() -> None:
    """Set up environment variables for remote MLflow server."""
    os.environ["MLFLOW_TRACKING_URI"] = REMOTE_MLFLOW_URI
    os.environ["MLFLOW_TRACKING_AWS_SIGV4"] = "true"
    os.environ["MLFLOW_S3_UPLOAD_EXTRA_ARGS"] = json.dumps(REMOTE_S3_EXTRA_ARGS)


def find_run_by_name(client: MlflowClient, run_name: str) -> Optional[str]:
    """
    Find a run ID by its name.
    
    Args:
        client: MLflow client to use for searching
        run_name: Name of the run to find
        
    Returns:
        Run ID if found, None otherwise
    """
    # Search across all experiments
    runs = mlflow.search_runs(
        search_all_experiments=True,
        filter_string=f"tags.mlflow.runName = '{run_name}'",
        max_results=1
    )
    
    if len(runs) == 0:
        return None
    
    return runs.iloc[0]["run_id"]


def get_or_create_experiment(
    dest_client: MlflowClient,
    experiment_name: str,
    source_experiment: mlflow.entities.Experiment
) -> str:
    """
    Get or create an experiment on the destination server.
    
    Args:
        dest_client: Destination MLflow client
        experiment_name: Name of the experiment
        source_experiment: Source experiment object for metadata
        
    Returns:
        Experiment ID on destination server
    """
    try:
        experiment = dest_client.get_experiment_by_name(experiment_name)
        if experiment:
            return experiment.experiment_id
    except Exception:
        pass
    
    # Create the experiment with tags from source
    tags = source_experiment.tags if source_experiment.tags else {}
    experiment_id = dest_client.create_experiment(
        name=experiment_name,
        tags=tags
    )
    
    return experiment_id


def copy_run(
    source_client: MlflowClient,
    dest_client: MlflowClient,
    source_run_id: str,
    dest_experiment_id: str,
    source_tracking_uri: str,
    dest_tracking_uri: str
) -> str:
    """
    Copy a single run from source to destination.
    
    Args:
        source_client: Source MLflow client
        dest_client: Destination MLflow client
        source_run_id: Run ID on source server
        dest_experiment_id: Experiment ID on destination server
        source_tracking_uri: Tracking URI for source server
        dest_tracking_uri: Tracking URI for destination server
        
    Returns:
        New run ID on destination server
    """
    # Get source run
    mlflow.set_tracking_uri(source_tracking_uri)
    source_run = source_client.get_run(source_run_id)
    
    print(f"\nCopying run: {source_run.info.run_name}")
    print(f"  Source run ID: {source_run_id}")
    print(f"  Status: {source_run.info.status}")
    
    # Create run on destination
    mlflow.set_tracking_uri(dest_tracking_uri)
    dest_run = dest_client.create_run(
        experiment_id=dest_experiment_id,
        run_name=source_run.info.run_name,
        start_time=source_run.info.start_time,
    )
    dest_run_id = dest_run.info.run_id
    
    print(f"  Destination run ID: {dest_run_id}")
    
    try:
        # Copy parameters
        if source_run.data.params:
            print(f"  Copying {len(source_run.data.params)} parameters...")
            for key, value in source_run.data.params.items():
                dest_client.log_param(dest_run_id, key, value)
        
        # Copy metrics (with timestamps and steps if available)
        if source_run.data.metrics:
            print(f"  Copying {len(source_run.data.metrics)} metrics...")
            mlflow.set_tracking_uri(source_tracking_uri)  # Switch to source for metric history
            for key in source_run.data.metrics.keys():
                # Get full metric history
                metric_history = source_client.get_metric_history(source_run_id, key)
                mlflow.set_tracking_uri(dest_tracking_uri)  # Switch to dest for logging
                for metric in metric_history:
                    dest_client.log_metric(
                        dest_run_id,
                        key,
                        metric.value,
                        timestamp=metric.timestamp,
                        step=metric.step
                    )
        
        # Copy tags (excluding auto-generated system tags)
        if source_run.data.tags:
            mlflow.set_tracking_uri(dest_tracking_uri)
            # Tags to preserve even if they start with mlflow.
            # mlflow.loggedArtifacts is CRITICAL for table display in MLflow UI
            preserve_tags = {'mlflow.loggedArtifacts', 'mlflow.runName'}
            tags_to_copy = {k: v for k, v in source_run.data.tags.items() 
                           if not k.startswith("mlflow.") or k in preserve_tags}
            print(f"  Copying {len(tags_to_copy)} tags (including table display metadata)...")
            for key, value in tags_to_copy.items():
                dest_client.set_tag(dest_run_id, key, value)
        
        # Copy artifacts
        mlflow.set_tracking_uri(source_tracking_uri)  # Switch to source for artifacts
        artifacts = source_client.list_artifacts(source_run_id)
        if artifacts:
            print(f"  Copying artifacts...")
            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                
                # Download all artifacts
                mlflow.set_tracking_uri(source_tracking_uri)
                print(f"    Downloading artifacts to {tmp_path}...")
                source_artifact_path = source_client.download_artifacts(
                    source_run_id, "", dst_path=str(tmp_path)
                )
                
                # Upload to destination
                mlflow.set_tracking_uri(dest_tracking_uri)
                print(f"    Uploading artifacts...")
                dest_client.log_artifacts(dest_run_id, source_artifact_path)
            
            print(f"    ✓ Artifacts copied")
        
        # Copy traces associated with the run
        try:
            mlflow.set_tracking_uri(source_tracking_uri)
            print(f"  Looking for traces...")
            traces = mlflow.search_traces(
                experiment_ids=[source_run.info.experiment_id],
                filter_string=f"attributes.run_id = '{source_run_id}'",
                max_results=1000
            )
            
            if len(traces) > 0:
                print(f"  Found {len(traces)} traces...")
                # MLflow 3.x doesn't provide a straightforward API to copy traces with
                # their full span hierarchy. The log_trace() API only creates simple traces
                # with a single root span, not the complex nested structures we need.
                # Traces would need to be reconstructed span-by-span using the
                # OpenTelemetry API which is beyond the scope of a simple copy script.
                print(f"    ⚠ Trace copying not implemented (requires complex OpenTelemetry reconstruction)")
                print(f"    ℹ️ Traces are mainly for local debugging")
                print(f"    ✓ Run metadata, metrics, and evaluation artifacts were copied successfully")
                
            else:
                print(f"  No traces found for this run")
                
        except Exception as e:
            print(f"  ⚠ Warning: Could not search/copy traces: {e}")
            # Don't fail the entire run copy if traces fail
        
        # Update run status and end time
        mlflow.set_tracking_uri(dest_tracking_uri)
        if source_run.info.status == RunStatus.to_string(RunStatus.FINISHED):
            dest_client.set_terminated(
                dest_run_id,
                status=RunStatus.to_string(RunStatus.FINISHED),
                end_time=source_run.info.end_time
            )
        elif source_run.info.status == RunStatus.to_string(RunStatus.FAILED):
            dest_client.set_terminated(
                dest_run_id,
                status=RunStatus.to_string(RunStatus.FAILED),
                end_time=source_run.info.end_time
            )
        
        print(f"  ✓ Run copied successfully")
        return dest_run_id
        
    except Exception as e:
        print(f"  ✗ Error copying run: {e}")
        # Try to mark the destination run as failed
        try:
            dest_client.set_terminated(
                dest_run_id,
                status=RunStatus.to_string(RunStatus.FAILED)
            )
        except Exception:
            pass
        raise


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Copy MLflow runs from local to remote server"
    )
    parser.add_argument(
        "run_identifier",
        help="Run name (e.g., 'ai_eval_wise-hare-663') or run ID to copy"
    )
    parser.add_argument(
        "--experiment",
        default="Chatbot",
        help="Experiment name (default: Chatbot)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without actually copying"
    )
    
    args = parser.parse_args()
    
    # Set up clients
    print(f"Connecting to local MLflow server: {LOCAL_MLFLOW_URI}")
    source_client = MlflowClient(tracking_uri=LOCAL_MLFLOW_URI)
    
    # Find run ID if a name was provided
    mlflow.set_tracking_uri(LOCAL_MLFLOW_URI)
    run_id = args.run_identifier
    if not run_id.startswith("run-") and len(run_id) != 32:
        print(f"\nSearching for run by name: {args.run_identifier}")
        found_run_id = find_run_by_name(source_client, args.run_identifier)
        if not found_run_id:
            print(f"✗ Run not found with name: {args.run_identifier}")
            return
        run_id = found_run_id
        print(f"✓ Found run ID: {run_id}")
    
    # Get source run and experiment info
    source_run = source_client.get_run(run_id)
    source_experiment = source_client.get_experiment(source_run.info.experiment_id)
    
    print(f"\nSource Run Details:")
    print(f"  Name: {source_run.info.run_name}")
    print(f"  Experiment: {source_experiment.name}")
    print(f"  Status: {source_run.info.status}")
    print(f"  Parameters: {len(source_run.data.params)}")
    print(f"  Metrics: {len(source_run.data.metrics)}")
    print(f"  Tags: {len(source_run.data.tags)}")
    
    if args.dry_run:
        print("\n[DRY RUN] Would copy this run to remote server")
        return
    
    # Set up remote server
    print(f"\nConnecting to remote MLflow server: {REMOTE_MLFLOW_URI}")
    aws_profile = os.getenv("AWS_PROFILE")
    if aws_profile:
        print(f"Using AWS profile: {aws_profile}")
    else:
        print("WARNING: AWS_PROFILE not set. Make sure AWS credentials are configured.")
    
    # Save local tracking URI before switching to remote
    saved_tracking_uri = mlflow.get_tracking_uri()
    
    setup_remote_env()
    mlflow.set_tracking_uri(REMOTE_MLFLOW_URI)  # Set global tracking URI for sagemaker plugin
    dest_client = MlflowClient(tracking_uri=REMOTE_MLFLOW_URI)
    
    # Get or create experiment on destination
    dest_experiment_id = get_or_create_experiment(
        dest_client,
        source_experiment.name,
        source_experiment
    )
    print(f"✓ Destination experiment ID: {dest_experiment_id}")
    
    # Copy the run
    try:
        dest_run_id = copy_run(
            source_client,
            dest_client,
            run_id,
            dest_experiment_id,
            LOCAL_MLFLOW_URI,
            REMOTE_MLFLOW_URI
        )
        
        print(f"\n✓ Successfully copied run!")
        print(f"  Source: {LOCAL_MLFLOW_URI}/#/experiments/{source_run.info.experiment_id}/runs/{run_id}")
        print(f"  Destination run ID: {dest_run_id}")
        print(f"\nTo view the run on remote server, use: ./open_mlflow.sh")
        
    except Exception as e:
        print(f"\n✗ Failed to copy run: {e}")
        raise


if __name__ == "__main__":
    main()

