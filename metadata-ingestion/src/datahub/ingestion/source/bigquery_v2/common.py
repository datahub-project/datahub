from typing import Any, Dict, Optional

from google.cloud.logging_v2.client import Client as GCPLoggingClient

BQ_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BQ_DATE_SHARD_FORMAT = "%Y%m%d"

BQ_EXTERNAL_TABLE_URL_TEMPLATE = "https://console.cloud.google.com/bigquery?project={project}&ws=!1m5!1m4!4m3!1s{project}!2s{dataset}!3s{table}"
BQ_EXTERNAL_DATASET_URL_TEMPLATE = "https://console.cloud.google.com/bigquery?project={project}&ws=!1m4!1m3!3m2!1s{project}!2s{dataset}"


def _make_gcp_logging_client(
    project_id: Optional[str] = None, extra_client_options: Dict[str, Any] = {}
) -> GCPLoggingClient:
    # See https://github.com/googleapis/google-cloud-python/issues/2674 for
    # why we disable gRPC here.
    client_options = extra_client_options.copy()
    client_options["_use_grpc"] = False
    if project_id is not None:
        return GCPLoggingClient(**client_options, project=project_id)
    else:
        return GCPLoggingClient(**client_options)
