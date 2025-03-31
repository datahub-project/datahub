import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

from pydantic import BaseModel

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.hex.constants import (
    DATAHUB_API_PAGE_SIZE_DEFAULT,
    HEX_PLATFORM_URN,
)
from datahub.metadata._schema_classes import QueryPropertiesClass, QuerySubjectsClass
from datahub.metadata.urns import DatasetUrn, QueryUrn, SchemaFieldUrn
from datahub.sdk.main_client import DataHubClient

logger = logging.getLogger(__name__)

# Pattern to extract both project_id and workspace_name from Hex metadata in SQL comments
HEX_METADATA_PATTERN = r'-- Hex query metadata: \{.*?"project_id": "([^"]+)".*?"project_url": "https?://[^/]+/([^/]+)/hex/.*?\}'


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
    filtered_out_queries_no_subjects: int = 0
    total_queries: int = 0
    total_dataset_subjects: int = 0
    total_schema_field_subjects: int = 0
    num_calls_fetch_query_entities: int = 0


# The following models were Claude-generated from DataHubClient._graph.get_entities_v2 response
# To be exclusively used internally for the deserialization
# Everything is Optional because... who knows!


class AuditStamp(BaseModel):
    actor: Optional[str]
    time: Optional[int]


class Statement(BaseModel):
    value: Optional[str]
    language: Optional[str]


class QueryPropertiesValue(BaseModel):
    statement: Optional[Statement]
    source: Optional[str]
    lastModified: Optional[AuditStamp]
    created: Optional[AuditStamp]


class QuerySubjectEntity(BaseModel):
    entity: Optional[str]


class QuerySubjectsValue(BaseModel):
    subjects: List[QuerySubjectEntity]


class QueryProperties(BaseModel):
    value: Optional[QueryPropertiesValue]


class QuerySubjects(BaseModel):
    value: Optional[QuerySubjectsValue]


class AspectModel(BaseModel):
    queryProperties: Optional[QueryProperties] = None
    querySubjects: Optional[QuerySubjects] = None


class EntitiesResponse(Dict[QueryUrn, AspectModel]):
    """
    It extends Dict[QueryUrn, AspectModel] and adds a parse_obj classmethod that
    allows direct parsing from the dict response returned by DataHubClient._graph.get_entities_v2.

    key = query urn
    """

    @classmethod
    def parse_obj(cls, obj: Dict[str, Any]) -> "EntitiesResponse":
        result = cls()
        for urn, aspects_dict in obj.items():
            try:
                # Parse the aspects data with our model
                aspects = AspectModel.parse_obj(aspects_dict)
                # Add to our dictionary
                result[QueryUrn.from_string(urn)] = aspects
            except Exception as e:
                logger.warning(f"Failed to parse aspects for {urn}: {e}")

        return result


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

    def fetch(self) -> Generator[QueryResponse, None, None]:
        try:
            query_urns = self._fetch_query_urns_filter_hex_and_last_modified()
            if not query_urns:
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
            for query_urn, aspects in entities_by_urn.items():
                # Skip if missing required aspects
                if (
                    not aspects.queryProperties
                    or not aspects.queryProperties.value
                    or not aspects.queryProperties.value.statement
                    or not aspects.queryProperties.value.statement.value
                    or not aspects.querySubjects
                    or not aspects.querySubjects.value
                    or not aspects.querySubjects.value.subjects
                ):
                    logger.debug(
                        f"Skipping query {query_urn} - missing required aspects or values: {aspects}"
                    )
                    self.report.filtered_out_queries_missing_metadata += 1
                    continue

                # Extract SQL statement to check for Hex metadata
                sql_statement = aspects.queryProperties.value.statement.value

                # Extract hex metadata (project_id and workspace_name)
                metadata_result = self._extract_hex_metadata(sql_statement)
                if not metadata_result:
                    logger.debug(
                        f"Skipping query {query_urn} - failed to extract Hex metadata"
                    )
                    self.report.filtered_out_queries_missing_metadata += 1
                    continue

                hex_project_id, workspace_from_url = metadata_result

                # Validate workspace
                if workspace_from_url != self.workspace_name:
                    logger.debug(
                        f"Skipping query {query_urn} - workspace '{workspace_from_url}' doesn't match '{self.workspace_name}'"
                    )
                    self.report.filtered_out_queries_different_workspace += 1
                    continue

                # Extract subjects
                dataset_subjects: List[DatasetUrn] = []
                schema_field_subjects: List[SchemaFieldUrn] = []
                for subject in aspects.querySubjects.value.subjects:
                    if subject.entity and subject.entity.startswith("urn:li:dataset:"):
                        dataset_subjects.append(DatasetUrn.from_string(subject.entity))
                    elif subject.entity and subject.entity.startswith(
                        "urn:li:schemaField:"
                    ):
                        schema_field_subjects.append(
                            SchemaFieldUrn.from_string(subject.entity)
                        )

                if not dataset_subjects and not schema_field_subjects:
                    self.report.filtered_out_queries_no_subjects += 1
                    continue

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
                yield response

    def _fetch_query_entities(self, query_urns: List[QueryUrn]) -> EntitiesResponse:
        entities_by_urn = EntitiesResponse()

        for i in range(0, len(query_urns), self.page_size):
            batch = query_urns[i : i + self.page_size]

            logger.debug(f"Fetching query entities for {len(batch)} queries: {batch}")
            response_json = self.datahub_client._graph.get_entities_v2(
                entity_name="query",
                urns=[urn.urn() for urn in batch],
                aspects=[
                    QueryPropertiesClass.ASPECT_NAME,
                    QuerySubjectsClass.ASPECT_NAME,
                ],
                with_system_metadata=False,
            )
            self.report.num_calls_fetch_query_entities += 1
            logger.debug(f"Get entities v2 response: {response_json}")
            batch_entities_by_urn = EntitiesResponse.parse_obj(response_json)
            entities_by_urn.update(batch_entities_by_urn)

        return entities_by_urn

    def _fetch_query_urns_filter_hex_and_last_modified(self) -> List[QueryUrn]:
        """
        This could be implemented as follows, but search sdk still requires some fixes.
        query_urns = self.datahub_client.search.get_urns(
            filter=F.and_(
                F.entity_type(QueryUrn.ENTITY_TYPE),
                F.custom_filter("origin", "EQUAL", [HEX_PLATFORM_URN.urn()]),
                F.custom_filter(
                    "lastModifiedAt",
                    "GREATER_THAN",
                    [str(last_modified_start_at_millis)],
                ),
            ),
        )
        """
        last_modified_start_at_millis = int(self.start_datetime.timestamp() * 1000)
        last_modified_end_at_millis = int(self.end_datetime.timestamp() * 1000)

        query_urn_strs = self.datahub_client._graph.get_urns_by_filter(
            entity_types=[QueryUrn.ENTITY_TYPE],
            batch_size=self.page_size,
            extraFilters=[
                {
                    "field": "origin",
                    "condition": "EQUAL",
                    "values": [HEX_PLATFORM_URN.urn()],
                    "negated": False,
                },
                {
                    "field": "lastModifiedAt",
                    "condition": "GREATER_THAN_OR_EQUAL_TO",
                    "values": [
                        last_modified_start_at_millis,
                    ],
                    "negated": False,
                },
                {
                    "field": "lastModifiedAt",
                    "condition": "LESS_THAN_OR_EQUAL_TO",
                    "values": [
                        last_modified_end_at_millis,
                    ],
                    "negated": False,
                },
            ],
        )
        query_urns = [QueryUrn.from_string(urn_str) for urn_str in query_urn_strs]
        logger.debug(f"Get URNS by filter: {query_urns}")
        return query_urns

    def _extract_hex_metadata(self, sql_statement: str) -> Optional[tuple[str, str]]:
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
            return None

        try:
            project_id = match.group(1)
            workspace_name = match.group(2)
            return project_id, workspace_name
        except (IndexError, AttributeError) as e:
            self.report.warning(
                title="Failed to extract information from Hex metadata",
                message="Failed to extract information from Hex metadata will result on missing lineage",
                context=sql_statement,
                exc=e,
            )

        return None
