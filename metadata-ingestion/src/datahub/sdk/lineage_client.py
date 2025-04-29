from __future__ import annotations

import difflib
from typing import TYPE_CHECKING, List, Literal, Optional, Set, Union

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    MetadataChangeProposalClass,
    QueryLanguageClass,
    QueryPropertiesClass,
    QuerySourceClass,
    QueryStatementClass,
    SchemaMetadataClass,
    UpstreamClass,
)
from datahub.metadata.urns import DatasetUrn, QueryUrn
from datahub.sdk._shared import DatajobUrnOrStr, DatasetUrnOrStr
from datahub.sdk._utils import DEFAULT_ACTOR_URN
from datahub.sdk.dataset import ColumnLineageMapping, parse_cll_mapping
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.fingerprint_utils import generate_hash
from datahub.utilities.ordered_set import OrderedSet

if TYPE_CHECKING:
    from datahub.sdk.main_client import DataHubClient


_empty_audit_stamp = AuditStampClass(
    time=0,
    actor=DEFAULT_ACTOR_URN,
)


class LineageClient:
    def __init__(self, client: DataHubClient):
        self._client = client

    def _get_fields_from_dataset_urn(self, dataset_urn: DatasetUrn) -> Set[str]:
        schema_metadata = self._client._graph.get_aspect(
            str(dataset_urn), SchemaMetadataClass
        )
        if schema_metadata is None:
            return Set()

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
        elif column_lineage in ["auto_fuzzy", "auto_strict"]:
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

        updater = DatasetPatchBuilder(str(downstream))
        updater.add_upstream_lineage(
            UpstreamClass(
                dataset=str(upstream),
                type=DatasetLineageTypeClass.COPY,
            )
        )
        for cl in cll or []:
            updater.add_fine_grained_upstream_lineage(cl)

        self._client.entities.update(updater)

    def add_dataset_transform_lineage(
        self,
        *,
        upstream: DatasetUrnOrStr,
        downstream: DatasetUrnOrStr,
        column_lineage: Optional[ColumnLineageMapping] = None,
        query_text: Optional[str] = None,
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
        if query_text:
            # Eventually we might want to use our regex-based fingerprinting instead.
            fingerprint = generate_hash(query_text)
            query_urn = QueryUrn(fingerprint).urn()

            from datahub.sql_parsing.sql_parsing_aggregator import make_query_subjects

            query_entity = MetadataChangeProposalWrapper.construct_many(
                query_urn,
                aspects=[
                    QueryPropertiesClass(
                        statement=QueryStatementClass(
                            value=query_text, language=QueryLanguageClass.SQL
                        ),
                        source=QuerySourceClass.SYSTEM,
                        created=_empty_audit_stamp,
                        lastModified=_empty_audit_stamp,
                    ),
                    make_query_subjects(list(fields_involved)),
                ],
            )

        updater = DatasetPatchBuilder(str(downstream))
        updater.add_upstream_lineage(
            UpstreamClass(
                dataset=str(upstream),
                type=DatasetLineageTypeClass.TRANSFORMED,
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
            Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
        ] = list(updater.build())
        if query_entity:
            mcps.extend(query_entity)
        self._client._graph.emit_mcps(mcps)

    def add_dataset_lineage_from_sql(
        self,
        *,
        query_text: str,
        platform: str,
        platform_instance: Optional[str] = None,
        env: str = "PROD",
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
        schema_aware: bool = True,
    ) -> None:
        """Add lineage by parsing a SQL query."""
        from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

        # Parse the SQL query to extract lineage information
        parsed_result = create_lineage_sql_parsed_result(
            query=query_text,
            default_db=default_db or "",
            default_schema=default_schema or "",
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=self._client._graph,
            schema_aware=schema_aware,
        )

        # Validate parsing result
        if not parsed_result:
            raise ValueError("Failed to parse SQL query: Unable to parse")
        if parsed_result.debug_info.error:
            raise ValueError(
                f"Failed to parse SQL query: {parsed_result.debug_info.error}"
            )
        if not parsed_result.out_tables:
            raise ValueError(
                "No output tables found in the query. Cannot establish lineage."
            )

        # Use the first output table as the downstream
        downstream_urn = parsed_result.out_tables[0]

        # Process all upstream tables found in the query
        for i, upstream_table in enumerate(parsed_result.in_tables):
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

            # Add lineage, including query text only for the first upstream
            self.add_dataset_transform_lineage(
                upstream=upstream_table,
                downstream=downstream_urn,
                column_lineage=column_mapping or None,
                query_text=query_text if i == 0 else None,
            )  # TODO: this throws error when table does not exist

    def add_datajob_lineage(
        self,
        *,
        upstream: Union[DatasetUrnOrStr, DatajobUrnOrStr],
        downstream: Union[DatasetUrnOrStr, DatajobUrnOrStr],
    ) -> None:
        """
        Add lineage between datasets and datajobs.

        Args:
            upstream: The upstream dataset or datajob (source)
            downstream: The downstream dataset or datajob (destination)
        """

        upstream_str = str(upstream)
        downstream_str = str(downstream)

        # Determine entity types
        upstream_is_dataset = upstream_str.startswith("urn:li:dataset:")
        downstream_is_dataset = downstream_str.startswith("urn:li:dataset:")

        # Only handle Dataset → DataJob and DataJob → Dataset cases
        if upstream_is_dataset and not downstream_is_dataset:
            # Dataset → DataJob: Add dataset as input to the job
            datajob_io = self._client._graph.get_aspect(
                downstream_str, DataJobInputOutputClass
            ) or DataJobInputOutputClass(inputDatasets=[], outputDatasets=[])

            # Add the dataset to inputs if not already present
            if upstream_str not in datajob_io.inputDatasets:
                datajob_io.inputDatasets.append(upstream_str)

            # Create and emit MCP - create a new instance directly
            mcp = MetadataChangeProposalWrapper(
                entityUrn=downstream_str,
                aspect=datajob_io,
            )
            self._client._graph.emit_mcp(mcp)

        elif not upstream_is_dataset and downstream_is_dataset:
            # DataJob → Dataset: Add dataset as output to the job
            datajob_io = self._client._graph.get_aspect(
                upstream_str, DataJobInputOutputClass
            ) or DataJobInputOutputClass(inputDatasets=[], outputDatasets=[])

            # Add the dataset to outputs if not already present
            if downstream_str not in datajob_io.outputDatasets:
                datajob_io.outputDatasets.append(downstream_str)

            # Create and emit MCP - create a new instance directly
            mcp = MetadataChangeProposalWrapper(
                entityUrn=upstream_str,
                aspect=datajob_io,
            )
            self._client._graph.emit_mcp(mcp)

        else:
            # Only accept Dataset → DataJob or DataJob → Dataset connections
            raise ValueError(
                "Can only create lineage between a dataset and a datajob. "
                "For dataset to dataset lineage, use add_dataset_transform_lineage instead."
            )
