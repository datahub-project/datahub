from __future__ import annotations

import difflib
import logging
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Union,
    overload,
)

from typing_extensions import assert_never, deprecated

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.metadata.urns import DataJobUrn, DatasetUrn, QueryUrn, SchemaFieldUrn, Urn
from datahub.sdk._shared import (
    ChartUrnOrStr,
    DashboardUrnOrStr,
    DatajobUrnOrStr,
    DatasetUrnOrStr,
)
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.dataset import ColumnLineageMapping, parse_cll_mapping
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, FilterDsl
from datahub.specific.chart import ChartPatchBuilder
from datahub.specific.dashboard import DashboardPatchBuilder
from datahub.specific.datajob import DataJobPatchBuilder
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.fingerprint_utils import generate_hash
from datahub.utilities.ordered_set import OrderedSet
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


_empty_audit_stamp = models.AuditStampClass(
    time=0,
    actor=DEFAULT_ACTOR_URN,
)


logger = logging.getLogger(__name__)


@dataclass
class LineagePath:
    urn: str
    entity_name: str
    column_name: Optional[str] = None


@dataclass
class LineageResult:
    urn: str
    type: str
    hops: int
    direction: Literal["upstream", "downstream"]
    platform: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    paths: Optional[List[LineagePath]] = None


class LineageClient:
    def __init__(self, client: DataHubClient):
        self._client = client
        self._graph = client._graph

    def _get_fields_from_dataset_urn(self, dataset_urn: DatasetUrn) -> Set[str]:
        schema_metadata = self._client._graph.get_aspect(
            str(dataset_urn), models.SchemaMetadataClass
        )
        if schema_metadata is None:
            return set()

        return {field.fieldPath for field in schema_metadata.fields}

    @classmethod
    def _get_strict_column_lineage(
        cls,
        upstream_fields: Set[str],
        downstream_fields: Set[str],
    ) -> ColumnLineageMapping:
        """Find matches between upstream and downstream fields with case-insensitive matching."""
        strict_column_lineage: ColumnLineageMapping = {}

        # Create case-insensitive mapping of upstream fields
        case_insensitive_map = {field.lower(): field for field in upstream_fields}

        # Match downstream fields using case-insensitive comparison
        for downstream_field in downstream_fields:
            lower_field = downstream_field.lower()
            if lower_field in case_insensitive_map:
                # Use the original case of the upstream field
                strict_column_lineage[downstream_field] = [
                    case_insensitive_map[lower_field]
                ]

        return strict_column_lineage

    @classmethod
    def _get_fuzzy_column_lineage(
        cls,
        upstream_fields: Set[str],
        downstream_fields: Set[str],
    ) -> ColumnLineageMapping:
        """Generate fuzzy matches between upstream and downstream fields."""

        # Simple normalization function for better matching
        def normalize(s: str) -> str:
            return s.lower().replace("_", "")

        # Create normalized lookup for upstream fields
        normalized_upstream = {normalize(field): field for field in upstream_fields}

        fuzzy_column_lineage = {}
        for downstream_field in downstream_fields:
            # Try exact match first
            if downstream_field in upstream_fields:
                fuzzy_column_lineage[downstream_field] = [downstream_field]
                continue

            # Try normalized match
            norm_downstream = normalize(downstream_field)
            if norm_downstream in normalized_upstream:
                fuzzy_column_lineage[downstream_field] = [
                    normalized_upstream[norm_downstream]
                ]
                continue

            # If no direct match, find closest match using similarity
            matches = difflib.get_close_matches(
                norm_downstream,
                normalized_upstream.keys(),
                n=1,  # Return only the best match
                cutoff=0.8,  # Adjust cutoff for sensitivity
            )

            if matches:
                fuzzy_column_lineage[downstream_field] = [
                    normalized_upstream[matches[0]]
                ]

        return fuzzy_column_lineage

    @overload
    def add_lineage(
        self,
        *,
        upstream: DatasetUrnOrStr,
        downstream: DatasetUrnOrStr,
        column_lineage: Union[
            bool, ColumnLineageMapping, Literal["auto_fuzzy", "auto_strict"]
        ] = False,
        transformation_text: Optional[str] = None,
    ) -> None:
        ...

        """
        Add dataset-to-dataset lineage with column-level mapping.
        """

    @overload
    def add_lineage(
        self,
        *,
        upstream: Union[DatajobUrnOrStr],
        downstream: DatasetUrnOrStr,
    ) -> None:
        ...

        """
        Add dataset-to-datajob or dataset-to-mlmodel lineage.
        """

    @overload
    def add_lineage(
        self,
        *,
        upstream: Union[DatasetUrnOrStr, DatajobUrnOrStr],
        downstream: DatajobUrnOrStr,
    ) -> None:
        ...

        """
        Add datajob-to-dataset or datajob-to-datajob lineage.
        """

    @overload
    def add_lineage(
        self,
        *,
        upstream: Union[DashboardUrnOrStr, DatasetUrnOrStr, ChartUrnOrStr],
        downstream: DashboardUrnOrStr,
    ) -> None:
        ...

        """
        Add dashboard-to-dashboard or dashboard-to-dataset lineage.
        """

    @overload
    def add_lineage(
        self,
        *,
        upstream: DatasetUrnOrStr,
        downstream: ChartUrnOrStr,
    ) -> None:
        ...
        """
        Add dataset-to-chart lineage.
        """

    # The actual implementation that handles all overloaded cases
    def add_lineage(
        self,
        *,
        upstream: Union[
            DatasetUrnOrStr, DatajobUrnOrStr, DashboardUrnOrStr, ChartUrnOrStr
        ],
        downstream: Union[
            DatasetUrnOrStr, DatajobUrnOrStr, DashboardUrnOrStr, ChartUrnOrStr
        ],
        column_lineage: Union[
            bool, ColumnLineageMapping, Literal["auto_fuzzy", "auto_strict"]
        ] = False,
        transformation_text: Optional[str] = None,
    ) -> None:
        """
        Add lineage between two entities.

        This flexible method handles different combinations of entity types:
        - dataset to dataset
        - dataset to datajob
        - datajob to dataset
        - datajob to datajob
        - dashboard to dataset
        - dashboard to chart
        - dashboard to dashboard
        - dataset to chart

        Args:
            upstream: URN of the upstream entity (dataset or datajob)
            downstream: URN of the downstream entity (dataset or datajob)
            column_lineage: Optional boolean to indicate if column-level lineage should be added or a lineage mapping type (auto_fuzzy, auto_strict, or a mapping of column-level lineage)
            transformation_text: Optional SQL query text that defines the transformation
                    (only applicable for dataset-to-dataset lineage)

        Raises:
            InvalidUrnError: If the URNs provided are invalid
            SdkUsageError: If certain parameter combinations are not supported
        """
        # Validate parameter combinations
        upstream_entity_type = Urn.from_string(upstream).entity_type
        downstream_entity_type = Urn.from_string(downstream).entity_type

        key = (upstream_entity_type, downstream_entity_type)

        # if it's not dataset-dataset lineage but provided with column_lineage or transformation_text, raise an error
        if key != ("dataset", "dataset") and (column_lineage or transformation_text):
            raise SdkUsageError(
                "Column lineage and query text are only applicable for dataset-to-dataset lineage"
            )

        lineage_handlers: dict[tuple[str, str], Callable] = {
            ("dataset", "dataset"): self._add_dataset_lineage,
            ("dataset", "dashboard"): self._add_dashboard_lineage,
            ("chart", "dashboard"): self._add_dashboard_lineage,
            ("dashboard", "dashboard"): self._add_dashboard_lineage,
            ("dataset", "dataJob"): self._add_datajob_lineage,
            ("dataJob", "dataJob"): self._add_datajob_lineage,
            ("dataJob", "dataset"): self._add_datajob_output,
            ("dataset", "chart"): self._add_chart_lineage,
        }

        try:
            lineage_handler = lineage_handlers[key]
            lineage_handler(
                upstream=upstream,
                downstream=downstream,
                upstream_type=upstream_entity_type,
                column_lineage=column_lineage,
                transformation_text=transformation_text,
            )
        except KeyError:
            raise SdkUsageError(
                f"Unsupported entity type combination: {upstream_entity_type} -> {downstream_entity_type}"
            ) from None

    def _add_dataset_lineage(
        self,
        *,
        upstream,
        downstream,
        column_lineage,
        transformation_text,
        **_,
    ):
        upstream_urn = DatasetUrn.from_string(upstream)
        downstream_urn = DatasetUrn.from_string(downstream)

        if column_lineage:
            column_lineage = (
                "auto_fuzzy" if column_lineage is True else column_lineage
            )  # if column_lineage is True, set it to auto_fuzzy
            cll = self._process_column_lineage(
                column_lineage, upstream_urn, downstream_urn
            )
        else:
            cll = None

        if transformation_text:
            self._process_transformation_lineage(
                transformation_text, upstream_urn, downstream_urn, cll
            )
        else:
            updater = DatasetPatchBuilder(str(downstream_urn))
            updater.add_upstream_lineage(
                models.UpstreamClass(
                    dataset=str(upstream_urn),
                    type=models.DatasetLineageTypeClass.COPY,
                )
            )
            for cl in cll or []:
                updater.add_fine_grained_upstream_lineage(cl)
            self._client.entities.update(updater)

    def _add_dashboard_lineage(self, *, upstream, downstream, upstream_type, **_):
        patch = DashboardPatchBuilder(str(downstream))
        if upstream_type == "dataset":
            patch.add_dataset_edge(upstream)
        elif upstream_type == "chart":
            patch.add_chart_edge(upstream)
        elif upstream_type == "dashboard":
            patch.add_dashboard(upstream)
        else:
            raise SdkUsageError(
                f"Unsupported entity type combination: {upstream_type} -> dashboard"
            )
        self._client.entities.update(patch)

    def _add_datajob_lineage(self, *, upstream, downstream, upstream_type, **_):
        patch = DataJobPatchBuilder(str(downstream))
        if upstream_type == "dataset":
            patch.add_input_dataset(upstream)
        elif upstream_type == "dataJob":
            patch.add_input_datajob(upstream)
        else:
            raise SdkUsageError(
                f"Unsupported entity type combination: {upstream_type} -> dataJob"
            )
        self._client.entities.update(patch)

    def _add_datajob_output(self, *, upstream, downstream, **_):
        patch = DataJobPatchBuilder(str(upstream))
        patch.add_output_dataset(downstream)
        self._client.entities.update(patch)

    def _add_chart_lineage(self, *, upstream, downstream, **_):
        patch = ChartPatchBuilder(str(downstream))
        patch.add_input_edge(upstream)
        self._client.entities.update(patch)

    def _process_column_lineage(self, column_lineage, upstream_urn, downstream_urn):
        cll = None
        if column_lineage:
            # Auto column lineage generation
            if column_lineage == "auto_fuzzy" or column_lineage == "auto_strict":
                upstream_schema = self._get_fields_from_dataset_urn(upstream_urn)
                downstream_schema = self._get_fields_from_dataset_urn(downstream_urn)

                # Choose matching strategy
                mapping = (
                    self._get_fuzzy_column_lineage(upstream_schema, downstream_schema)
                    if column_lineage == "auto_fuzzy"
                    else self._get_strict_column_lineage(
                        upstream_schema, downstream_schema
                    )
                )
                cll = parse_cll_mapping(
                    upstream=upstream_urn,
                    downstream=downstream_urn,
                    cll_mapping=mapping,
                )
                # Explicit column lineage
            elif isinstance(column_lineage, dict):
                cll = parse_cll_mapping(
                    upstream=upstream_urn,
                    downstream=downstream_urn,
                    cll_mapping=column_lineage,
                )
            else:
                assert_never(column_lineage)
        return cll

    def _process_transformation_lineage(
        self, transformation_text, upstream_urn, downstream_urn, cll
    ):
        fields_involved = OrderedSet([str(upstream_urn), str(downstream_urn)])
        if cll is not None:
            for c in cll:
                for field in c.upstreams or []:
                    fields_involved.add(field)
                for field in c.downstreams or []:
                    fields_involved.add(field)

                # Create query URN and entity
        query_urn = QueryUrn(generate_hash(transformation_text)).urn()
        from datahub.sql_parsing.sql_parsing_aggregator import (
            make_query_subjects,
        )

        query_entity = MetadataChangeProposalWrapper.construct_many(
            query_urn,
            aspects=[
                models.QueryPropertiesClass(
                    statement=models.QueryStatementClass(
                        value=transformation_text,
                        language=models.QueryLanguageClass.SQL,
                    ),
                    source=models.QuerySourceClass.SYSTEM,
                    created=_empty_audit_stamp,
                    lastModified=_empty_audit_stamp,
                ),
                make_query_subjects(list(fields_involved)),
            ],
        )

        # Build dataset update
        updater = DatasetPatchBuilder(str(downstream_urn))
        updater.add_upstream_lineage(
            models.UpstreamClass(
                dataset=str(upstream_urn),
                type=models.DatasetLineageTypeClass.TRANSFORMED,
                query=query_urn,
            )
        )

        # Add fine-grained lineage
        for cl in cll or []:
            cl.query = query_urn
            updater.add_fine_grained_upstream_lineage(cl)

            # Check dataset existence
        if not self._client._graph.exists(updater.urn):
            raise SdkUsageError(
                f"Dataset {updater.urn} does not exist, and hence cannot be updated."
            )

            # Emit metadata change proposals
        mcps: List[
            Union[
                MetadataChangeProposalWrapper,
                models.MetadataChangeProposalClass,
            ]
        ] = list(updater.build())
        if query_entity:
            mcps.extend(query_entity)
        self._client._graph.emit_mcps(mcps)

    def infer_lineage_from_sql(
        self,
        *,
        query_text: str,
        platform: str,
        platform_instance: Optional[str] = None,
        env: str = "PROD",
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
    ) -> None:
        """Add lineage by parsing a SQL query."""
        from datahub.sql_parsing.sqlglot_lineage import (
            create_lineage_sql_parsed_result,
        )

        # Parse the SQL query to extract lineage information
        parsed_result = create_lineage_sql_parsed_result(
            query=query_text,
            default_db=default_db,
            default_schema=default_schema,
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=self._client._graph,
        )

        if parsed_result.debug_info.table_error:
            raise SdkUsageError(
                f"Failed to parse SQL query: {parsed_result.debug_info.error}"
            )
        elif parsed_result.debug_info.column_error:
            logger.warning(
                f"Failed to parse SQL query: {parsed_result.debug_info.error}",
            )

        if not parsed_result.out_tables:
            raise SdkUsageError(
                "No output tables found in the query. Cannot establish lineage."
            )

        # Use the first output table as the downstream
        downstream_urn = parsed_result.out_tables[0]

        # Process all upstream tables found in the query
        for upstream_table in parsed_result.in_tables:
            # Skip self-lineage
            if upstream_table == downstream_urn:
                continue

            # Extract column-level lineage for this specific upstream table
            column_mapping = {}
            if parsed_result.column_lineage:
                for col_lineage in parsed_result.column_lineage:
                    if not (col_lineage.downstream and col_lineage.downstream.column):
                        continue

                    # Filter upstreams to only include columns from current upstream table
                    upstream_cols = [
                        ref.column
                        for ref in col_lineage.upstreams
                        if ref.table == upstream_table and ref.column
                    ]

                    if upstream_cols:
                        column_mapping[col_lineage.downstream.column] = upstream_cols

            # Add lineage, including query text
            self.add_lineage(
                upstream=upstream_table,
                downstream=downstream_urn,
                column_lineage=column_mapping,
                transformation_text=query_text,
            )

    @deprecated("Use add_lineage instead")
    def add_dataset_copy_lineage(
        self,
        *,
        upstream: DatasetUrnOrStr,
        downstream: DatasetUrnOrStr,
        column_lineage: Union[
            None, ColumnLineageMapping, Literal["auto_fuzzy", "auto_strict"]
        ] = "auto_fuzzy",
    ) -> None:
        upstream = DatasetUrn.from_string(upstream)
        downstream = DatasetUrn.from_string(downstream)

        if column_lineage is None:
            cll = None
        elif column_lineage == "auto_fuzzy" or column_lineage == "auto_strict":
            upstream_schema = self._get_fields_from_dataset_urn(upstream)
            downstream_schema = self._get_fields_from_dataset_urn(downstream)
            if column_lineage == "auto_fuzzy":
                mapping = self._get_fuzzy_column_lineage(
                    upstream_schema, downstream_schema
                )
            else:
                mapping = self._get_strict_column_lineage(
                    upstream_schema, downstream_schema
                )
            cll = parse_cll_mapping(
                upstream=upstream,
                downstream=downstream,
                cll_mapping=mapping,
            )
        elif isinstance(column_lineage, dict):
            cll = parse_cll_mapping(
                upstream=upstream,
                downstream=downstream,
                cll_mapping=column_lineage,
            )
        else:
            assert_never(column_lineage)

        updater = DatasetPatchBuilder(str(downstream))
        updater.add_upstream_lineage(
            models.UpstreamClass(
                dataset=str(upstream),
                type=models.DatasetLineageTypeClass.COPY,
            )
        )
        for cl in cll or []:
            updater.add_fine_grained_upstream_lineage(cl)

        self._client.entities.update(updater)

    @deprecated("Use add_lineage instead")
    def add_dataset_transform_lineage(
        self,
        *,
        upstream: DatasetUrnOrStr,
        downstream: DatasetUrnOrStr,
        column_lineage: Optional[ColumnLineageMapping] = None,
        transformation_text: Optional[str] = None,
    ) -> None:
        upstream = DatasetUrn.from_string(upstream)
        downstream = DatasetUrn.from_string(downstream)

        cll = None
        if column_lineage is not None:
            cll = parse_cll_mapping(
                upstream=upstream,
                downstream=downstream,
                cll_mapping=column_lineage,
            )

        fields_involved = OrderedSet([str(upstream), str(downstream)])
        if cll is not None:
            for c in cll:
                for field in c.upstreams or []:
                    fields_involved.add(field)
                for field in c.downstreams or []:
                    fields_involved.add(field)

        query_urn = None
        query_entity = None
        if transformation_text:
            # Eventually we might want to use our regex-based fingerprinting instead.
            fingerprint = generate_hash(transformation_text)
            query_urn = QueryUrn(fingerprint).urn()

            from datahub.sql_parsing.sql_parsing_aggregator import make_query_subjects

            query_entity = MetadataChangeProposalWrapper.construct_many(
                query_urn,
                aspects=[
                    models.QueryPropertiesClass(
                        statement=models.QueryStatementClass(
                            value=transformation_text,
                            language=models.QueryLanguageClass.SQL,
                        ),
                        source=models.QuerySourceClass.SYSTEM,
                        created=_empty_audit_stamp,
                        lastModified=_empty_audit_stamp,
                    ),
                    make_query_subjects(list(fields_involved)),
                ],
            )

        updater = DatasetPatchBuilder(str(downstream))
        updater.add_upstream_lineage(
            models.UpstreamClass(
                dataset=str(upstream),
                type=models.DatasetLineageTypeClass.TRANSFORMED,
                query=query_urn,
            )
        )
        for cl in cll or []:
            cl.query = query_urn
            updater.add_fine_grained_upstream_lineage(cl)

        # Throw if the dataset does not exist.
        # We need to manually call .build() instead of reusing client.update()
        # so that we make just one emit_mcps call.
        if not self._client._graph.exists(updater.urn):
            raise SdkUsageError(
                f"Dataset {updater.urn} does not exist, and hence cannot be updated."
            )

        mcps: List[
            Union[MetadataChangeProposalWrapper, models.MetadataChangeProposalClass]
        ] = list(updater.build())
        if query_entity:
            mcps.extend(query_entity)
        self._client._graph.emit_mcps(mcps)

    @deprecated("Use add_lineage instead")
    def add_datajob_lineage(
        self,
        *,
        datajob: DatajobUrnOrStr,
        upstreams: Optional[List[Union[DatasetUrnOrStr, DatajobUrnOrStr]]] = None,
        downstreams: Optional[List[DatasetUrnOrStr]] = None,
    ) -> None:
        """
        Add lineage between a datajob and datasets/datajobs.

        Args:
            datajob: The datajob URN to connect lineage with
            upstreams: List of upstream datasets or datajobs that serve as inputs to the datajob
            downstreams: List of downstream datasets that are outputs of the datajob
        """

        if not upstreams and not downstreams:
            raise SdkUsageError("No upstreams or downstreams provided")

        datajob_urn = DataJobUrn.from_string(datajob)

        # Initialize the patch builder for the datajob
        patch_builder = DataJobPatchBuilder(str(datajob_urn))

        # Process upstream connections (inputs to the datajob)
        if upstreams:
            for upstream in upstreams:
                # try converting to dataset urn
                try:
                    dataset_urn = DatasetUrn.from_string(upstream)
                    patch_builder.add_input_dataset(dataset_urn)
                except InvalidUrnError:
                    # try converting to datajob urn
                    datajob_urn = DataJobUrn.from_string(upstream)
                    patch_builder.add_input_datajob(datajob_urn)

        # Process downstream connections (outputs from the datajob)
        if downstreams:
            for downstream in downstreams:
                downstream_urn = DatasetUrn.from_string(downstream)
                patch_builder.add_output_dataset(downstream_urn)

        # Apply the changes to the entity
        self._client.entities.update(patch_builder)

    def get_lineage(
        self,
        *,
        source_urn: Union[str, Urn],
        source_column: Optional[str] = None,
        direction: Literal["upstream", "downstream"] = "upstream",
        max_hops: int = 1,
        filter: Optional[Filter] = None,
        count: int = 500,
    ) -> List[LineageResult]:
        """
        Retrieve lineage entities connected to a source entity.
        Args:
            source_urn: Source URN for the lineage search
            source_column: Source column for the lineage search
            direction: Direction of lineage traversal
            max_hops: Maximum number of hops to traverse
            filter: Filters to apply to the lineage search
            count: Maximum number of results to return

        Returns:
            List of lineage results

        Raises:
            SdkUsageError for invalid filter values
        """
        # Validate and convert input URN
        source_urn = Urn.from_string(source_urn)
        # Prepare GraphQL query variables with a separate method
        variables = self._process_input_variables(
            source_urn, source_column, filter, direction, max_hops, count
        )

        return self._execute_lineage_query(variables, direction)

    def _process_input_variables(
        self,
        source_urn: Urn,
        source_column: Optional[str] = None,
        filters: Optional[Filter] = None,
        direction: Literal["upstream", "downstream"] = "upstream",
        max_hops: int = 1,
        count: int = 500,
    ) -> Dict[str, Any]:
        """
        Process filters and prepare GraphQL query variables for lineage search.

        Args:
            source_urn: Source URN for the lineage search
            source_column: Source column for the lineage search
            filters: Optional filters to apply
            direction: Direction of lineage traversal
            max_hops: Maximum number of hops to traverse
            count: Maximum number of results to return

        Returns:
            Dictionary of GraphQL query variables

        Raises:
            SdkUsageError for invalid filter values
        """

        # print warning if max_hops is greater than 2
        if max_hops > 2:
            logger.warning(
                """If `max_hops` is more than 2, the search will try to find the full lineage graph.
                By default, only 500 results are shown.
                You can change the `count` to get more or fewer results.
                """
            )

        # Determine hop values
        max_hop_values = (
            [str(hop) for hop in range(1, max_hops + 1)]
            if max_hops <= 2
            else ["1", "2", "3+"]
        )

        degree_filter = FilterDsl.custom_filter(
            field="degree",
            condition="EQUAL",
            values=max_hop_values,
        )

        filters_with_max_hops = (
            FilterDsl.and_(degree_filter, filters)
            if filters is not None
            else degree_filter
        )

        types, compiled_filters = compile_filters(filters_with_max_hops)

        # Prepare base variables
        variables: Dict[str, Any] = {
            "input": {
                "urn": str(source_urn),
                "direction": direction.upper(),
                "count": count,
                "types": types,
                "orFilters": compiled_filters,
            }
        }

        # if column is provided, update the variables to include the schema field urn
        if isinstance(source_urn, SchemaFieldUrn) or source_column:
            variables["input"]["searchFlags"] = {
                "groupingSpec": {
                    "groupingCriteria": {
                        "baseEntityType": "SCHEMA_FIELD",
                        "groupingEntityType": "SCHEMA_FIELD",
                    }
                }
            }
        if isinstance(source_urn, SchemaFieldUrn):
            variables["input"]["urn"] = str(source_urn)
        elif source_column:
            variables["input"]["urn"] = str(SchemaFieldUrn(source_urn, source_column))

        return variables

    def _execute_lineage_query(
        self,
        variables: Dict[str, Any],
        direction: Literal["upstream", "downstream"],
    ) -> List[LineageResult]:
        """Execute GraphQL query and process results."""
        # Construct GraphQL query with dynamic path query
        graphql_query = """
        query scrollAcrossLineage($input: ScrollAcrossLineageInput!) {
            scrollAcrossLineage(input: $input) {
                nextScrollId
                searchResults {
                    degree
                    entity {
                        urn
                        type
                        ... on Dataset {
                            name
                            platform {
                                name
                            }
                            properties {
                                description
                            }
                        }
                        ... on DataJob {
                            jobId
                            dataPlatformInstance {
                                platform {
                                    name
                                }
                            }
                            properties {
                                name
                                description
                            }
                        }
                    }
                    paths {
                        path {
                            urn
                            type
                        }
                    }
                }
            }
        }
        """

        results: List[LineageResult] = []

        first_iter = True
        scroll_id: Optional[str] = None

        while first_iter or scroll_id:
            first_iter = False

            # Update scroll ID if applicable
            if scroll_id:
                variables["input"]["scrollId"] = scroll_id

            # Execute GraphQL query
            response = self._graph.execute_graphql(graphql_query, variables=variables)
            data = response["scrollAcrossLineage"]
            scroll_id = data.get("nextScrollId")

            # Process search results
            for entry in data["searchResults"]:
                entity = entry["entity"]

                result = self._create_lineage_result(entity, entry, direction)
                results.append(result)

        return results

    def _create_lineage_result(
        self,
        entity: Dict[str, Any],
        entry: Dict[str, Any],
        direction: Literal["upstream", "downstream"],
    ) -> LineageResult:
        """Create a LineageResult from entity and entry data."""
        platform = entity.get("platform", {}).get("name") or entity.get(
            "dataPlatformInstance", {}
        ).get("platform", {}).get("name")

        result = LineageResult(
            urn=entity["urn"],
            type=entity["type"],
            hops=entry["degree"],
            direction=direction,
            platform=platform,
        )

        properties = entity.get("properties", {})
        if properties:
            result.name = properties.get("name", "")
            result.description = properties.get("description", "")

        result.paths = []
        if "paths" in entry:
            # Process each path in the lineage graph
            for path in entry["paths"]:
                for path_entry in path["path"]:
                    # Only include schema fields in the path (exclude other types like Query)
                    if path_entry["type"] == "SCHEMA_FIELD":
                        schema_field_urn = SchemaFieldUrn.from_string(path_entry["urn"])
                        result.paths.append(
                            LineagePath(
                                urn=path_entry["urn"],
                                entity_name=DatasetUrn.from_string(
                                    schema_field_urn.parent
                                ).name,
                                column_name=schema_field_urn.field_path,
                            )
                        )

        return result
