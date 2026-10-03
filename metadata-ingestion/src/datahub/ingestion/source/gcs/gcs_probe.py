"""Metadata-only probe over Google Cloud Storage, through the S3-compatible
client config and the s3:// path_spec rewrite GCSSource ingests with. The
listings live in the shared S3-compatible base; this adds the bucket listing
and builds the base from a GCS recipe."""

from contextlib import contextmanager
from typing import Iterator, List, Optional, Sequence

import google.auth.exceptions
import requests

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.data_lake_common.object_store_probe import (
    S3CompatibleMetadataProbe,
    Whole,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.gcs.gcs_source import (
    GCSAuthType,
    GCSOAuthAwsConnectionConfig,
    GCSSourceConfig,
    build_gcs_aws_connection_config,
    equivalent_s3_path_specs,
)

# Longer than any one bounded listing takes; WIF / ADC tokens live an hour.
_TOKEN_REFRESH_MARGIN_SECONDS = 300.0


class GCSMetadataProbe(S3CompatibleMetadataProbe):
    """Lists GCS the way GCSSource does, and never reads an object's contents."""

    display_scheme = "gs://"

    def __init__(
        self,
        aws_config: AwsConnectionConfig,
        path_specs: Sequence[PathSpec],
        auth_type: GCSAuthType,
        refusal: Optional[str] = None,
    ) -> None:
        super().__init__(aws_config, path_specs, refusal=refusal)
        self._auth_type = auth_type

    @contextmanager
    def _storage_errors(self, context: str, whole: Whole) -> Iterator[None]:
        """The base's split, plus the OAuth token refresh (WIF / ADC), which
        fails outside botocore's error types. google-auth's text carries the
        subject-token path, the token endpoint's response body and the
        principal, so only the class name is reported."""
        try:
            # Refreshed here, before botocore sends anything: a refresh that
            # fails inside the before-send hook is logged by botocore with
            # google-auth's text, which no message scrubbing here can reach.
            # Ahead of expiry, so a listing does not cross it and leave the
            # hook to refresh mid-way.
            if isinstance(self._aws_config, GCSOAuthAwsConnectionConfig):
                self._aws_config.refresh_token_if_needed(
                    margin_seconds=_TOKEN_REFRESH_MARGIN_SECONDS
                )
            with super()._storage_errors(context, whole):
                yield
        except (
            google.auth.exceptions.GoogleAuthError,
            requests.exceptions.RequestException,
        ) as exc:
            raise ProbeConnectionError(
                f"{context}: could not refresh GCS credentials for auth_type "
                f"'{self._auth_type}' ({type(exc).__name__})"
            ) from None

    @classmethod
    def for_config(cls, config: GCSSourceConfig) -> "GCSMetadataProbe":
        # No request is made here: boto builds its client on the first listing,
        # and OAuth tokens refresh in the before-send hook on the first request.
        # Loading WIF / ADC credentials reads local configuration only.
        try:
            aws_config = build_gcs_aws_connection_config(config)
        except Exception as exc:
            # The google-auth errors ingestion wraps carry file paths and
            # parser text from the credential material; report the class only,
            # and do not chain the original, which a traceback would print.
            raise ProbeConnectionError(
                f"could not load GCS credentials for auth_type "
                f"'{config.auth_type}' ({type(exc).__name__}); check the "
                f"recipe's credential settings and the environment they read"
            ) from None
        return cls(
            aws_config=aws_config,
            path_specs=equivalent_s3_path_specs(config.path_specs),
            auth_type=config.auth_type,
        )

    @probe_method(kind=DatasetContainerSubTypes.GCS_BUCKET, row_limit_param="limit")
    def buckets(self, limit: int = 200) -> List[str]:
        """Buckets this credential can list in its project (an HMAC key's
        project, or x-goog-project-id for WIF / ADC), whether or not a
        path_spec reaches them -- judge them with `probe filter --kind "GCS
        bucket"`. Needs storage.buckets.list: a credential scoped to single
        buckets reports a failure, not an empty list."""
        return self._list_buckets(limit)
