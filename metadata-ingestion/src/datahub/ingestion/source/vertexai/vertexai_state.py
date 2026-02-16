from typing import Dict, List


class ModelUsageTracker:
    """Tracks which models are used by downstream jobs for lineage."""

    def __init__(self) -> None:
        self._model_to_downstream_jobs: Dict[str, List[str]] = {}

    def track_usage(self, model_urn: str, job_urn: str) -> None:
        if model_urn not in self._model_to_downstream_jobs:
            self._model_to_downstream_jobs[model_urn] = []
        if job_urn not in self._model_to_downstream_jobs[model_urn]:
            self._model_to_downstream_jobs[model_urn].append(job_urn)

    def get_downstream_jobs(self, model_urn: str) -> List[str]:
        return self._model_to_downstream_jobs.get(model_urn, [])

    def clear(self) -> None:
        self._model_to_downstream_jobs.clear()
