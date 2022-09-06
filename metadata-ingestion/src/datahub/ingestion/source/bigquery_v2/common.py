from typing import Any, Dict, Optional

from google.cloud.logging_v2.client import Client as GCPLoggingClient

BQ_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BQ_DATE_SHARD_FORMAT = "%Y%m%d"


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
