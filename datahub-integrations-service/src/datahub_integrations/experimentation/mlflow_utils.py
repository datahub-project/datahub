import mlflow
from mlflow.entities import Run


def get_most_recent_run_for_expt(experiment_name: str) -> Run:
    """Get the most recent MLflow run name."""
    runs = mlflow.search_runs(
        experiment_names=[experiment_name],
        order_by=["start_time DESC"],
        max_results=1,
        output_format="list",
    )
    assert isinstance(runs, list)
    if not runs:
        raise ValueError("No runs found in experiment")
    return runs[0]
