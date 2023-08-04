from typing import Any, Dict, Optional

from google.cloud import bigquery
from google.cloud.logging_v2.client import Client as GCPLoggingClient
from google.oauth2.credentials import Credentials

from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config

BQ_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
BQ_DATE_SHARD_FORMAT = "%Y%m%d"

BQ_EXTERNAL_TABLE_URL_TEMPLATE = "https://console.cloud.google.com/bigquery?project={project}&ws=!1m5!1m4!4m3!1s{project}!2s{dataset}!3s{table}"
BQ_EXTERNAL_DATASET_URL_TEMPLATE = "https://console.cloud.google.com/bigquery?project={project}&ws=!1m4!1m3!3m2!1s{project}!2s{dataset}"


def _make_gcp_logging_client(
    config: BigQueryV2Config, project_id: Optional[str] = None
) -> GCPLoggingClient:
    # See https://github.com/googleapis/google-cloud-python/issues/2674 for
    # why we disable gRPC here.
    client_options = config.extra_client_options.copy()
    client_options["_use_grpc"] = False
    if project_id is not None:
        if config.credential_type == "cloudsdk_auth_access_token":
            credentials = Credentials(config.gcp_token.token)
            return GCPLoggingClient(credentials=credentials, project=config.project_on_behalf or config.gcp_token.project_id,)
        return GCPLoggingClient(**client_options, project=project_id)
    else:
        if config.use_cloudsdk_auth_access_token:
            credentials = Credentials(config.cloudsdk_auth_access_token)
            return GCPLoggingClient(credentials=credentials)
        return GCPLoggingClient(**client_options)


def get_bigquery_client(config: BigQueryV2Config) -> bigquery.Client:
    if config.credential_type == "cloudsdk_auth_access_token":
        credentials = Credentials(config.gcp_token.token)
        return bigquery.Client(config.project_on_behalf or config.gcp_token.project_id, credentials=credentials)
    
    client_options = config.extra_client_options
    return bigquery.Client(config.project_on_behalf, **client_options)


def get_sql_alchemy_url(config: BigQueryV2Config) -> str:
    if config.project_on_behalf:
        return f"bigquery://{config.project_on_behalf}"
    # When project_id is not set, we will attempt to detect the project ID
    # based on the credentials or environment variables.
    # See https://github.com/mxmzdlv/pybigquery#authentication.
    return "bigquery://"
