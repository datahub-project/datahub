from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Literal, NoReturn, Optional

from datahub.errors import SdkUsageError

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


class AssertionClient:
    """Client for externally managed (CUSTOM) assertions.

    Use this for third-party / self-reported data quality checks. Do not use
    native typed assertion models (FIELD, VOLUME, FRESHNESS, DATA_SCHEMA, SQL)
    for external self-reporting — those are for assertions DataHub evaluates
    or schedules natively.
    """

    def __init__(self, client: DataHubClient):
        self._client = client
        self._graph = client._graph

    def __getattr__(self, name: str) -> NoReturn:
        # This client stands in for the Cloud AssertionsClient when
        # acryl-datahub-cloud is absent, so anything it does not implement is a
        # Cloud-only API. Point at the install rather than an AttributeError.
        if name.startswith("_"):
            raise AttributeError(name)
        raise SdkUsageError(
            "AssertionsClient is not installed, please install it with "
            f"`pip install acryl-datahub-cloud` to use {name}. Without it, only "
            "sync_custom_assertion and report_assertion_result are available."
        )

    def sync_custom_assertion(
        self,
        *,
        entity_urn: str,
        type: str,
        description: str,
        platform_name: Optional[str] = None,
        platform_urn: Optional[str] = None,
        urn: Optional[str] = None,
        field_path: Optional[str] = None,
        field_paths: Optional[List[str]] = None,
        external_url: Optional[str] = None,
        logic: Optional[str] = None,
        scope: Optional[str] = None,
        aggregation: Optional[str] = None,
        operator: Optional[str] = None,
        parameters: Optional[Dict] = None,
        native_type: Optional[str] = None,
        native_parameters: Optional[List[Dict[str, str]]] = None,
    ) -> Dict:
        """Create or update a CUSTOM assertion with optional structured fields.

        Args:
            entity_urn: Dataset URN the assertion monitors.
            type: UI category for the assertion (appears as customType).
            description: Human-readable description.
            platform_name: Data platform name when platform_urn is not known.
            platform_urn: Data platform URN (preferred over platform_name).
            urn: Optional stable assertion URN. Preserves run history across syncs.
            field_path: Optional single column path (compat). Prefer field_paths.
            field_paths: Optional list of column paths for multi-column checks.
            external_url: Link to the external monitoring tool.
            logic: Optional native logic / SQL string.
            scope: Optional DatasetAssertionScope value (e.g. DATASET_COLUMN).
            aggregation: Optional AssertionStdAggregation value.
            operator: Optional AssertionStdOperator value.
            parameters: Optional AssertionStdParametersInput-shaped dict.
            native_type: Platform-specific assertion type string.
            native_parameters: Optional list of {key, value} maps.

        Returns:
            GraphQL response containing the assertion urn.
        """
        return self._graph.upsert_custom_assertion(
            urn=urn,
            entity_urn=entity_urn,
            type=type,
            description=description,
            platform_name=platform_name,
            platform_urn=platform_urn,
            field_path=field_path,
            field_paths=field_paths,
            external_url=external_url,
            logic=logic,
            scope=scope,
            aggregation=aggregation,
            operator=operator,
            parameters=parameters,
            native_type=native_type,
            native_parameters=native_parameters,
        )

    def report_assertion_result(
        self,
        urn: str,
        timestamp_millis: int,
        type: Literal["SUCCESS", "FAILURE", "ERROR", "INIT"],
        properties: Optional[List[Dict[str, str]]] = None,
        external_url: Optional[str] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        severity: Optional[Literal["LOW", "MEDIUM", "HIGH"]] = None,
    ) -> bool:
        """Report a run result for an assertion."""
        return self._graph.report_assertion_result(
            urn=urn,
            timestamp_millis=timestamp_millis,
            type=type,
            properties=properties,
            external_url=external_url,
            error_type=error_type,
            error_message=error_message,
            severity=severity,
        )
