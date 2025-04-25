import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.hex.constants import (
    DATAHUB_API_PAGE_SIZE_DEFAULT,
    HEX_PLATFORM_URN,
)
from datahub.metadata.schema_classes import QueryPropertiesClass, QuerySubjectsClass
from datahub.metadata.urns import DatasetUrn, QueryUrn, SchemaFieldUrn
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F
from datahub.utilities.time import datetime_to_ts_millis

logger = logging.getLogger(__name__)

# Pattern to extract both project_id and workspace_name from Hex metadata in SQL comments
# Only match metadata with "context": "SCHEDULED_RUN" to filter out non-scheduled runs
HEX_METADATA_PATTERN = r'-- Hex query metadata: \{.*?"context": "SCHEDULED_RUN".*?"project_id": "([^"]+)".*?"project_url": "https?://[^/]+/([^/]+)/hex/.*?\}'


@dataclass
class QueryResponse:
    """This is the public response model for the HexQueryFetcher."""

    urn: QueryUrn
    hex_project_id: str
    dataset_subjects: List[DatasetUrn] = field(default_factory=list)
    schema_field_subjects: List[SchemaFieldUrn] = field(default_factory=list)


@dataclass
class HexQueryFetcherReport(SourceReport):
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    fetched_query_urns: int = 0
    fetched_query_objects: int = 0
    filtered_out_queries_missing_metadata: int = 0
    filtered_out_queries_different_workspace: int = 0
    filtered_out_queries_no_match: int = 0
    filtered_out_queries_no_subjects: int = 0
    total_queries: int = 0
    total_dataset_subjects: int = 0
    total_schema_field_subjects: int = 0
    num_calls_fetch_query_entities: int = 0


class HexQueryFetcher:
    def __init__(
        self,
        datahub_client: DataHubClient,
        workspace_name: str,
        start_datetime: datetime,
        end_datetime: datetime,
        report: HexQueryFetcherReport,
        page_size: int = DATAHUB_API_PAGE_SIZE_DEFAULT,
    ):
        self.datahub_client = datahub_client
        self.workspace_name = workspace_name
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.report = report
        self.page_size = page_size

        self.report.start_datetime = start_datetime
        self.report.end_datetime = end_datetime

    def fetch(self) -> Iterable[QueryResponse]:
        try:
            query_urns = self._fetch_query_urns_filter_hex_and_last_modified()
            assert all(isinstance(urn, QueryUrn) for urn in query_urns)
            self.report.fetched_query_urns = len(query_urns)

            entities_by_urn = self._fetch_query_entities(query_urns)
            self.report.fetched_query_objects = len(entities_by_urn)
        except Exception as e:
            self.report.failure(
                title="Error fetching Queries for lineage",
                message="Error fetching Queries will result on missing lineage",
                context=str(
                    dict(
                        workspace_name=self.workspace_name,
                        start_datetime=self.start_datetime,
                        end_datetime=self.end_datetime,
                    )
                ),
                exc=e,
            )
        else:
            if not query_urns or not entities_by_urn:
                self.report.warning(
                    title="No Queries found with Hex as origin",
                    message="No lineage because of no Queries found with Hex as origin in the given time range; you may consider extending the time range to fetch more queries.",
                    context=str(
                        dict(
                            workspace_name=self.workspace_name,
                            start_datetime=self.start_datetime,
                            end_datetime=self.end_datetime,
                        )
                    ),
                )
                return

            for query_urn, (
                query_properties,
                query_subjects,
            ) in entities_by_urn.items():
                maybe_query_response = self._build_query_response(
                    query_urn=query_urn,
                    query_properties=query_properties,
                    query_subjects=query_subjects,
                )
                if maybe_query_response:
                    yield maybe_query_response

    def _fetch_query_entities(
        self, query_urns: List[QueryUrn]
    ) -> Dict[
        QueryUrn, Tuple[Optional[QueryPropertiesClass], Optional[QuerySubjectsClass]]
    ]:
        entities_by_urn: Dict[
            QueryUrn,
            Tuple[Optional[QueryPropertiesClass], Optional[QuerySubjectsClass]],
        ] = {}
        for i in range(0, len(query_urns), self.page_size):
            batch = query_urns[i : i + self.page_size]

            logger.debug(f"Fetching query entities for {len(batch)} queries: {batch}")
            entities = self.datahub_client._graph.get_entities(
                entity_name=QueryUrn.ENTITY_TYPE,
                urns=[urn.urn() for urn in batch],
                aspects=[
                    QueryPropertiesClass.ASPECT_NAME,
                    QuerySubjectsClass.ASPECT_NAME,
                ],
                with_system_metadata=False,
            )
            self.report.num_calls_fetch_query_entities += 1
            logger.debug(f"Get entities response: {entities}")

            for urn, entity in entities.items():
                query_urn = QueryUrn.from_string(urn)

                properties_tuple = entity.get(
                    QueryPropertiesClass.ASPECT_NAME, (None, None)
                )
                query_properties: Optional[QueryPropertiesClass] = None
                if properties_tuple and properties_tuple[0]:
                    assert isinstance(properties_tuple[0], QueryPropertiesClass)
                    query_properties = properties_tuple[0]

                subjects_tuple = entity.get(
                    QuerySubjectsClass.ASPECT_NAME, (None, None)
                )
                query_subjects: Optional[QuerySubjectsClass] = None
                if subjects_tuple and subjects_tuple[0]:
                    assert isinstance(subjects_tuple[0], QuerySubjectsClass)
                    query_subjects = subjects_tuple[0]

                entities_by_urn[query_urn] = (query_properties, query_subjects)

        return entities_by_urn

    def _fetch_query_urns_filter_hex_and_last_modified(self) -> List[QueryUrn]:
        last_modified_start_at_millis = datetime_to_ts_millis(self.start_datetime)
        last_modified_end_at_millis = datetime_to_ts_millis(self.end_datetime)

        urns = self.datahub_client.search.get_urns(
            filter=F.and_(
                F.entity_type(QueryUrn.ENTITY_TYPE),
                F.custom_filter("origin", "EQUAL", [HEX_PLATFORM_URN.urn()]),
                F.custom_filter(
                    "lastModifiedAt",
                    "GREATER_THAN_OR_EQUAL_TO",
                    [str(last_modified_start_at_millis)],
                ),
                F.custom_filter(
                    "lastModifiedAt",
                    "LESS_THAN_OR_EQUAL_TO",
                    [str(last_modified_end_at_millis)],
                ),
            ),
        )
        logger.debug(f"Get URNS by filter: {urns}")
        return [QueryUrn.from_string(urn.urn()) for urn in urns]

    def _extract_hex_metadata(self, sql_statement: str) -> Optional[Tuple[str, str]]:
        """
        Extract project ID and workspace name from SQL statement.

        Looks for Hex metadata in SQL comments in the format:
        -- Hex query metadata: {"project_id": "...", "project_url": "https://app.hex.tech/{workspace_name}/hex/..."}

        Example:
        -- Hex query metadata: {"categories": ["Scratchpad"], "cell_type": "SQL", "connection": "Long Tail Companions", "context": "SCHEDULED_RUN", "project_id": "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf", "project_url": "https://app.hex.tech/acryl-partnership/hex/d73da67d-c87b-4dd8-9e7f-b79cb7f822cf/draft/logic?selectedCellId=67c38da0-e631-4005-9750-5bdae2a2ef3f"}

        # TODO: Consider supporting multiline metadata format in the future:
        # -- Hex query metadata: {
        # --   "categories": ["Scratchpad"],
        # --   "project_id": "d73da67d-c87b-4dd8-9e7f-b79cb7f822cf",
        # --   ...
        # -- }

        Returns:
            A tuple of (project_id, workspace_name) if both are successfully extracted
            None if extraction fails for any reason
        """
        # Extract both project_id and workspace name in a single regex operation
        match = re.search(HEX_METADATA_PATTERN, sql_statement)

        if not match:
            self.report.filtered_out_queries_no_match += 1
            return None

        try:
            project_id = match.group(1)
            workspace_name = match.group(2)
            return project_id, workspace_name
        except (IndexError, AttributeError) as e:
            self.report.warning(
                title="Failed to extract information from Hex query metadata",
                message="Failed to extract information from Hex query metadata will result on missing lineage",
                context=sql_statement,
                exc=e,
            )

        return None

    def _build_query_response(
        self,
        query_urn: QueryUrn,
        query_properties: Optional[QueryPropertiesClass],
        query_subjects: Optional[QuerySubjectsClass],
    ) -> Optional[QueryResponse]:
        # Skip if missing required aspects
        if (
            not query_properties
            or not query_properties.statement
            or not query_properties.statement.value
            or not query_subjects
            or query_subjects.subjects is None  # empty list is allowed
        ):
            logger.debug(
                f"Skipping query {query_urn} - missing required fields: {(query_properties, query_subjects)}"
            )
            self.report.filtered_out_queries_missing_metadata += 1
            return None

        # Extract hex metadata (project_id and workspace_name)
        metadata_result = self._extract_hex_metadata(query_properties.statement.value)
        if not metadata_result:
            logger.debug(f"Skipping query {query_urn} - failed to extract Hex metadata")
            self.report.filtered_out_queries_missing_metadata += 1
            return None

        hex_project_id, workspace_from_url = metadata_result

        # Validate workspace
        if workspace_from_url != self.workspace_name:
            logger.debug(
                f"Skipping query {query_urn} - workspace '{workspace_from_url}' doesn't match '{self.workspace_name}'"
            )
            self.report.filtered_out_queries_different_workspace += 1
            return None

        # Extract subjects
        dataset_subjects: List[DatasetUrn] = []
        schema_field_subjects: List[SchemaFieldUrn] = []
        for subject in query_subjects.subjects:
            if subject.entity and subject.entity.startswith("urn:li:dataset:"):
                dataset_subjects.append(DatasetUrn.from_string(subject.entity))
            elif subject.entity and subject.entity.startswith("urn:li:schemaField:"):
                schema_field_subjects.append(SchemaFieldUrn.from_string(subject.entity))

        if not dataset_subjects and not schema_field_subjects:
            self.report.filtered_out_queries_no_subjects += 1
            return None

        # Create response
        response = QueryResponse(
            urn=query_urn,
            hex_project_id=hex_project_id,
            dataset_subjects=dataset_subjects,
            schema_field_subjects=schema_field_subjects,
        )
        logger.debug(
            f"Succesfully extracted {len(dataset_subjects)} dataset subjects and {len(schema_field_subjects)} schema field subjects for query {query_urn}: {dataset_subjects} {schema_field_subjects}"
        )
        self.report.total_queries += 1
        self.report.total_dataset_subjects += len(dataset_subjects)
        self.report.total_schema_field_subjects += len(schema_field_subjects)

        logger.debug(
            f"Processed query {query_urn} with Hex project ID {hex_project_id}"
        )

        return response
