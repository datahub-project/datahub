"""Metadata-only probe over Google Cloud Storage, through the S3-compatible
client config and the s3:// path_spec rewrite GCSSource ingests with. The
listings live in the shared S3-compatible base; this adds the bucket listing
and builds the base from a GCS recipe."""

from typing import List

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.data_lake_common.object_store_probe import (
    S3CompatibleMetadataProbe,
)
from datahub.ingestion.source.gcs.gcs_source import (
    GCSSourceConfig,
    build_gcs_aws_connection_config,
    equivalent_s3_path_specs,
)


class GCSMetadataProbe(S3CompatibleMetadataProbe):
    """Lists GCS the way GCSSource does, and never reads an object's contents."""

    display_scheme = "gs://"

    @classmethod
    def for_config(cls, config: GCSSourceConfig) -> "GCSMetadataProbe":
        # No request is made here: boto builds its client on the first listing,
        # and OAuth tokens refresh in the before-send hook on the first request.
        # Loading WIF / ADC credentials reads local configuration only.
        try:
            aws_config = build_gcs_aws_connection_config(config)
        except Exception as exc:
            # The google-auth errors ingestion wraps carry file paths and
            # parser text from the credential material; report the class only.
            raise ProbeConnectionError(
                f"could not load GCS credentials for auth_type "
                f"'{config.auth_type}' ({type(exc).__name__}); check the "
                f"recipe's credential settings and the environment they read"
            ) from exc
        return cls(
            aws_config=aws_config,
            path_specs=equivalent_s3_path_specs(config.path_specs),
        )

    @probe_method(kind=DatasetContainerSubTypes.GCS_BUCKET, row_limit_param="limit")
    def buckets(self, limit: int = 200) -> List[str]:
        """Buckets this credential can list in its project (an HMAC key's
        project, or x-goog-project-id for WIF / ADC), whether or not a
        path_spec reaches them -- judge them with `probe filter --kind "GCS
        bucket"`. Needs storage.buckets.list: a credential scoped to single
        buckets reports a failure, not an empty list."""
        return self._list_buckets(limit)
