"""Encapsulated handling for Google Sheets connectors.

DataHub doesn't natively support Google Sheets as a source platform yet,
so the Fivetran connector emits `Dataset` entities for the sheet itself
(and a named-range dataset for each connector) and uses sheet-aware URNs
as the upstream side of lineage edges. This module keeps that workaround
in one place — once DataHub adds native Google Sheets support, delete
this file plus the two delegation points in `FivetranSource` and the
workaround disappears cleanly.

References:
- Constant.GOOGLE_SHEETS_CONNECTOR_TYPE: the Fivetran service string we
  match on.
- DatasetSubTypes.GOOGLE_SHEETS / GOOGLE_SHEETS_NAMED_RANGE: subtypes
  used by the emitted Dataset entities.
"""

import logging
from typing import Callable, Dict, Iterable, Optional
from urllib.parse import urlparse

from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.fivetran.config import (
    Constant,
    FivetranSourceConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.data_classes import Connector
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import (
    FivetranConnectionDetails,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.schema_classes import DatasetLineageTypeClass, UpstreamClass
from datahub.metadata.urns import DatasetUrn
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

_CORPUSER_DATAHUB = "urn:li:corpuser:datahub"


class GoogleSheetsConnectorHandler:
    """Workaround for Google Sheets connectors lacking native DataHub support.

    Owns:
    - Fetching connector details from the Fivetran REST API (cached
      per-instance — these calls are only used for Google Sheets right
      now, so the cache lives here rather than on `FivetranSource`).
    - Building the input dataset URN for a Google Sheets lineage row.
    - Emitting `Dataset` entities for the sheet and named range.

    Callers should check `applies_to(connector.connector_type)` first
    rather than calling the building methods unconditionally.
    """

    def __init__(
        self,
        api_client_provider: Callable[[], Optional[FivetranAPIClient]],
        config: FivetranSourceConfig,
        report: FivetranSourceReport,
    ) -> None:
        # `api_client_provider` is read lazily on each call so the
        # handler always reflects the source's current `api_client`
        # — important for tests that swap the client post-init, and
        # safe for production where the client never changes.
        self._api_client_provider = api_client_provider
        self._config = config
        self._report = report
        # Cache lives here — `_get_connection_details_by_id` is only
        # called for Google Sheets connectors today, so colocating the
        # cache with the only caller keeps the API surface tight.
        self._connection_details_cache: Dict[str, FivetranConnectionDetails] = {}
        # One-shot guard: "API client not initialized" is a deployment
        # fact (the user didn't supply `api_config`), not a per-connector
        # problem. Emit it once per ingest with a list of affected
        # connectors instead of one warning per Google Sheets connector.
        self._api_client_unavailable_reported = False

    @staticmethod
    def applies_to(connector_type: str) -> bool:
        """True if this handler should be invoked for the given connector type."""
        return connector_type == Constant.GOOGLE_SHEETS_CONNECTOR_TYPE

    def build_input_dataset_urn(
        self, connector: Connector, env: str
    ) -> Optional[DatasetUrn]:
        """Build the source-side URN for a Google Sheets lineage edge.

        Returns `None` if the connector's sheet metadata cannot be
        resolved (API failure, missing api_client, malformed sheet URL).
        Caller should treat `None` as "skip this lineage edge."
        """
        logger.debug(
            "Processing Google Sheets connector for lineage extraction: "
            f"connector_name={connector.connector_name}, "
            f"connector_id={connector.connector_id}"
        )
        conn_details = self._get_connection_details(connector.connector_id)
        named_range = (
            self._get_named_range_dataset_id(conn_details) if conn_details else None
        )
        if named_range is None:
            # Only emit the per-connector warning when the cause is
            # something we haven't already reported deployment-wide.
            # `_api_client_unavailable_reported` covers the missing
            # `api_config` case — re-warning per connector would just
            # duplicate the one-shot above.
            if not self._api_client_unavailable_reported:
                self._report.warning(
                    title="Failed to extract lineage for Google Sheets Connector",
                    message=(
                        "Unable to extract lineage for Google Sheets Connector. "
                        "This may occur if: (1) connector details could not be "
                        "fetched from Fivetran API, or (2) the sheet URL format "
                        "is invalid or unsupported."
                    ),
                    context=(
                        f"{connector.connector_name} "
                        f"(connector_id: {connector.connector_id})"
                    ),
                )
            return None
        return DatasetUrn.create_from_ids(
            platform_id=Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
            table_name=named_range,
            env=env,
        )

    def emit_workunits(self, connector: Connector) -> Iterable[Entity]:
        """Yield `Dataset` entities representing the Google Sheet and its
        named range. The named-range dataset has the sheet as its upstream.
        """
        logger.debug(
            "Processing Google Sheets connector for workunit generation: "
            f"connector_name={connector.connector_name}, "
            f"connector_id={connector.connector_id}"
        )
        conn_details = self._get_connection_details(connector.connector_id)
        sheet_id = self._get_sheet_id_from_url(conn_details) if conn_details else None
        named_range = (
            self._get_named_range_dataset_id(conn_details) if conn_details else None
        )

        if not (conn_details and sheet_id and named_range):
            # Same one-shot suppression as `build_input_dataset_urn` —
            # don't re-warn per connector when the deployment-level
            # "no api_config" notice already covered the cause.
            if not self._api_client_unavailable_reported:
                self._report.warning(
                    title="Failed to generate entities for Google Sheets",
                    message=(
                        "Failed to generate Google Sheets dataset entities. "
                        "This may occur if: (1) connector details could not be "
                        "fetched from Fivetran API, or (2) the sheet URL format "
                        "is invalid or unsupported."
                    ),
                    context=(
                        f"{connector.connector_name} "
                        f"(connector_id: {connector.connector_id})"
                    ),
                )
            return

        gsheets_dataset = Dataset(
            name=sheet_id,
            platform=Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
            env=self._config.env,
            display_name=sheet_id,
            external_url=conn_details.config.sheet_id,
            created=conn_details.created_at,
            last_modified=conn_details.succeeded_at,
            subtype=DatasetSubTypes.GOOGLE_SHEETS,
            custom_properties={
                "ingested_by": "fivetran source",
                "connector_id": conn_details.id,
            },
        )
        gsheets_named_range_dataset = Dataset(
            name=named_range,
            platform=Constant.GOOGLE_SHEETS_CONNECTOR_TYPE,
            env=self._config.env,
            display_name=conn_details.config.named_range,
            external_url=conn_details.config.sheet_id,
            created=conn_details.created_at,
            last_modified=conn_details.succeeded_at,
            subtype=DatasetSubTypes.GOOGLE_SHEETS_NAMED_RANGE,
            custom_properties={
                "ingested_by": "fivetran source",
                "connector_id": conn_details.id,
            },
            upstreams=UpstreamLineage(
                upstreams=[
                    UpstreamClass(
                        dataset=str(gsheets_dataset.urn),
                        type=DatasetLineageTypeClass.VIEW,
                        auditStamp=AuditStamp(
                            time=int(conn_details.created_at.timestamp() * 1000),
                            actor=_CORPUSER_DATAHUB,
                        ),
                    )
                ],
                fineGrainedLineages=None,
            ),
        )

        yield gsheets_dataset
        yield gsheets_named_range_dataset

    # ------------------------------------------------------------------
    # Internal helpers — exposed as `_` methods so unit tests can drive
    # the URL parsing logic directly without standing up the full handler.

    def _get_connection_details(
        self, connection_id: str
    ) -> Optional[FivetranConnectionDetails]:
        """Fetch and cache connection details for a Google Sheets connector.

        Returns `None` (with a structured report entry) when the API client
        isn't configured or the REST call fails. Callers must handle `None`.
        """
        api_client = self._api_client_provider()
        if api_client is None:
            # One-shot warning: the cause is "no api_config in the
            # recipe", which is the same for every Google Sheets
            # connector in the run. Subsequent calls silently return
            # None — callers know to skip the connector. Without this,
            # an account with N Google Sheets connectors would emit 2N
            # warnings (one here + one from the caller's failure path).
            if not self._api_client_unavailable_reported:
                self._report.warning(
                    title="Fivetran API client is not initialized",
                    message=(
                        "Google Sheets connector details cannot be extracted "
                        "without `api_config` in the recipe. The Google "
                        "Sheets workaround (sheet/named-range Dataset "
                        "entities + sheet-aware lineage URNs) is skipped "
                        "for all Google Sheets connectors in this run. "
                        "Add `api_config` with Fivetran API credentials "
                        "to enable it."
                    ),
                    context=f"first affected connector_id: {connection_id}",
                )
                self._api_client_unavailable_reported = True
            return None

        if connection_id in self._connection_details_cache:
            return self._connection_details_cache[connection_id]

        self._report.report_fivetran_rest_api_call_count()
        try:
            conn_details = api_client.get_connection_details_by_id(connection_id)
            self._connection_details_cache[connection_id] = conn_details
            return conn_details
        except Exception as e:
            logger.error(
                "Failed to get connection details using rest-api for "
                f"connector_id: {connection_id}. Error: {str(e)}",
                exc_info=True,
            )
            self._report.failure(
                title="Failed to get connection details for Google Sheets Connector",
                message=(
                    "Exception occurred while getting connection details "
                    "from Fivetran API"
                ),
                context=f"connector_id: {connection_id}",
                exc=e,
            )
            return None

    def _get_sheet_id_from_url(
        self, conn_details: FivetranConnectionDetails
    ) -> Optional[str]:
        """Extract the Google Sheets ID from `config.sheet_id`.

        Handles both shapes Fivetran returns:
        1. Full URL: ``https://docs.google.com/spreadsheets/d/<id>/edit?...``
        2. Bare ID: ``<id>``
        """
        sheet_id = conn_details.config.sheet_id

        # Already just an ID — no URL prefix.
        if not sheet_id.startswith(("http://", "https://")):
            return sheet_id

        # URL form. Path layout: /spreadsheets/d/<id>/edit
        try:
            parsed = urlparse(sheet_id)
            parts = parsed.path.split("/")
            if len(parts) > 3 and parts[2] == "d":
                return parts[3]
            logger.warning(
                f"Unexpected URL format for sheet_id: {sheet_id}. "
                "Expected format: https://docs.google.com/spreadsheets/d/<id>/..."
            )
        except Exception as e:
            logger.warning(
                f"Failed to extract sheet_id from URL: {sheet_id}, Error: {e}"
            )
        return None

    def _get_named_range_dataset_id(
        self, conn_details: FivetranConnectionDetails
    ) -> Optional[str]:
        """Compose `<sheet_id>.<named_range>` for use as a DatasetUrn name."""
        sheet_id = self._get_sheet_id_from_url(conn_details)
        if sheet_id is None:
            return None
        named_range_id = (
            f"{sheet_id}.{conn_details.config.named_range}"
            if sheet_id
            else conn_details.config.named_range
        )
        logger.debug(
            f"Using gsheet_named_range_dataset_id: {named_range_id} "
            f"for connector: {conn_details.id}"
        )
        return named_range_id
