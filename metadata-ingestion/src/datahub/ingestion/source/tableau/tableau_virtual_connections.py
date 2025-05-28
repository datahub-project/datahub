import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp_builder import add_entity_to_container
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau_common import (
    clean_table_name,
    create_vc_schema_field_v2,
    create_vc_table_schema_field_v2,
    virtual_connection_detailed_graphql_query,
    virtual_connection_graphql_query,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    DatasetSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    OtherSchema,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)


class VirtualConnectionProcessor:
    """Handles Virtual Connection processing for Tableau connector"""

    def __init__(self, tableau_source):
        self.tableau_source = tableau_source
        self.config = tableau_source.config
        self.report = tableau_source.report
        self.server = tableau_source.server
        self.platform = tableau_source.platform
        self.ctx = tableau_source.ctx

        # VC tracking data structures
        self.vc_table_ids_for_lookup: List[str] = []
        self.vc_table_id_to_vc_id: Dict[str, str] = {}
        self.vc_table_id_to_name: Dict[str, str] = {}
        self.virtual_connection_ids_being_used: List[str] = []
        self.datasource_vc_relationships: Dict[str, List[Dict[str, Any]]] = {}
        self.vc_table_column_types: Dict[str, str] = {}

    def process_datasource_for_vc_refs(
        self, datasource: dict, datasource_type: str
    ) -> None:
        """Process a single datasource for VC references - called during datasource emission"""
        datasource_id = datasource.get(c.ID)
        if not datasource_id:
            return

        vc_references = []

        fields = datasource.get(c.FIELDS, [])
        for field in fields:
            field_name = field.get(c.NAME)
            if not field_name:
                continue

            upstream_columns = field.get(c.UPSTREAM_COLUMNS, [])
            for upstream_col in upstream_columns:
                table = upstream_col.get(c.TABLE, {})
                table_type = table.get(c.TYPE_NAME)

                if table_type == c.VIRTUAL_CONNECTION_TABLE:
                    vc_table_id = table.get(c.ID)
                    vc_table_name = table.get(c.NAME)
                    column_name = upstream_col.get(c.NAME)

                    # Get VC info if available
                    vc_info = table.get("virtualConnection", {})
                    vc_id = vc_info.get(c.ID) if vc_info else None

                    logger.debug(
                        f"Found VC reference: field={field_name}, "
                        f"vc_table={vc_table_name}, column={column_name}"
                    )

                    vc_references.append(
                        {
                            "field_name": field_name,
                            "vc_table_id": vc_table_id,
                            "vc_table_name": vc_table_name,
                            "column_name": column_name,
                            "vc_id": vc_id,
                        }
                    )

                    # Collect VC table IDs for lookup
                    if vc_table_id and vc_table_id not in self.vc_table_ids_for_lookup:
                        self.vc_table_ids_for_lookup.append(vc_table_id)

        # Store relationships
        if vc_references:
            self.datasource_vc_relationships[datasource_id] = vc_references
            logger.debug(
                f"Stored {len(vc_references)} VC relationships for datasource {datasource_id}"
            )

    def lookup_vc_ids_from_table_ids(self) -> None:
        """Step 2: Lookup VC IDs from VC table IDs and store mappings"""
        if not self.vc_table_ids_for_lookup:
            logger.info("No VC table IDs to lookup")
            return

        logger.info(f"Looking up {len(self.vc_table_ids_for_lookup)} VC table IDs")

        # Query all VCs to find matches
        for vc in self.tableau_source.get_connection_objects(
            query=virtual_connection_graphql_query,
            connection_type=c.VIRTUAL_CONNECTIONS_CONNECTION,
            query_filter={},  # Get all VCs
            page_size=self.config.effective_virtual_connection_page_size,
        ):
            vc_id = vc.get(c.ID)
            vc_name = vc.get(c.NAME, "Unknown")
            tables = vc.get("tables", [])

            for table in tables:
                table_id = table.get(c.ID)
                table_name = table.get(c.NAME)

                if table_id in self.vc_table_ids_for_lookup:
                    logger.debug(
                        f"Found VC table match: {table_name} (ID: {table_id}) in VC: {vc_name}"
                    )

                    # Build mappings
                    self.vc_table_id_to_vc_id[table_id] = vc_id
                    self.vc_table_id_to_name[table_id] = table_name

                    if vc_id not in self.virtual_connection_ids_being_used:
                        self.virtual_connection_ids_being_used.append(vc_id)

                    # Store column types for v2 field paths
                    columns = table.get(c.COLUMNS, [])
                    for column in columns:
                        col_name = column.get(c.NAME)
                        col_type = column.get(c.REMOTE_TYPE, c.UNKNOWN)
                        if col_name:
                            self.vc_table_column_types[f"{table_id}.{col_name}"] = (
                                col_type
                            )

        logger.info(
            f"Found {len(self.vc_table_id_to_vc_id)} VC table mappings, "
            f"will process {len(self.virtual_connection_ids_being_used)} VCs"
        )

    def emit_virtual_connections(self):
        """Emit Virtual Connection datasets with v2 schema fields"""
        if not self.virtual_connection_ids_being_used:
            logger.info("No Virtual Connections to emit")
            return

        vc_filter = {c.ID_WITH_IN: self.virtual_connection_ids_being_used}

        for vc in self.tableau_source.get_connection_objects(
            query=virtual_connection_detailed_graphql_query,
            connection_type=c.VIRTUAL_CONNECTIONS_CONNECTION,
            query_filter=vc_filter,
            page_size=self.config.effective_virtual_connection_page_size,
        ):
            yield from self._emit_single_virtual_connection(vc)

    def _emit_single_virtual_connection(self, vc: dict) -> Iterable[MetadataWorkUnit]:
        """Emit a single Virtual Connection dataset"""
        vc_id = vc[c.ID]
        vc_urn = builder.make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=vc_id,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        logger.debug(f"Emitting Virtual Connection: {vc.get(c.NAME)} ({vc_id})")

        # Create dataset snapshot
        dataset_snapshot = DatasetSnapshot(
            urn=vc_urn,
            aspects=[self.tableau_source.get_data_platform_instance()],
        )

        # Dataset properties
        dataset_props = DatasetPropertiesClass(
            name=vc.get(c.NAME),
            description=vc.get(c.DESCRIPTION),
            customProperties=self.tableau_source.get_custom_props_from_dict(
                vc, [c.LUID]
            ),
        )
        dataset_snapshot.aspects.append(dataset_props)

        # Schema metadata with v2 field paths grouped by table
        vc_tables = vc.get("tables", [])
        schema_metadata = self._get_vc_schema_metadata_grouped_by_table(vc_tables)
        if schema_metadata:
            dataset_snapshot.aspects.append(schema_metadata)

        # Create upstream lineage to database tables
        upstream_tables, fine_grained_lineages = self._create_vc_upstream_lineage_v2(
            vc, vc_tables, vc_urn
        )

        if upstream_tables:
            upstream_lineage = UpstreamLineage(
                upstreams=upstream_tables,
                fineGrainedLineages=fine_grained_lineages or None,
            )
            yield self.tableau_source.get_metadata_change_proposal(
                vc_urn,
                aspect_name=c.UPSTREAM_LINEAGE,
                aspect=upstream_lineage,
            )

        # Add to project container if available
        project_luid = self._get_vc_project_luid(vc)
        if project_luid:
            yield from add_entity_to_container(
                self.tableau_source.gen_project_key(project_luid),
                c.DATASET,
                dataset_snapshot.urn,
            )

        yield self.tableau_source.get_metadata_change_event(dataset_snapshot)
        yield self.tableau_source.get_metadata_change_proposal(
            dataset_snapshot.urn,
            aspect_name=c.SUB_TYPES,
            aspect=SubTypesClass(typeNames=["Virtual Connection"]),
        )

    def _get_vc_schema_metadata_grouped_by_table(
        self, vc_tables: List[dict]
    ) -> Optional[SchemaMetadata]:
        """Create schema metadata for VC with fields grouped by table using v2 specification"""
        fields = []

        # Group columns by table to ensure proper v2 field path creation
        table_columns = self._group_vc_columns_by_table(vc_tables)

        for table_name, table_info in table_columns.items():
            original_table_name = table_info["original_name"]
            columns = table_info["columns"]

            # Add table-level field (represents the table itself in the VC)
            table_field = create_vc_table_schema_field_v2(
                table_name=table_name, description=f"Table: {original_table_name}"
            )
            fields.append(table_field)

            # Add column-level fields under this table
            for column in columns:
                column_name = column.get(c.NAME)
                if not column_name:
                    self.report.num_datasource_field_skipped_no_name += 1
                    logger.warning(
                        f"Skipping VC column {column.get(c.ID)} from schema since its name is none"
                    )
                    continue

                column_type = column.get(c.REMOTE_TYPE, c.UNKNOWN)
                description = column.get(c.DESCRIPTION)

                # Create v2 schema field
                schema_field = create_vc_schema_field_v2(
                    table_name=table_name,
                    column_name=column_name,
                    column_type=column_type,
                    description=description,
                    ingest_tags=self.config.ingest_tags,
                )
                fields.append(schema_field)

        if not fields:
            return None

        return SchemaMetadata(
            schemaName="VirtualConnection",
            platform=f"urn:li:dataPlatform:{self.platform}",
            version=0,
            fields=fields,
            hash="",
            platformSchema=OtherSchema(rawSchema=""),
        )

    def _group_vc_columns_by_table(
        self, vc_tables: List[dict]
    ) -> Dict[str, Dict[str, Any]]:
        """Group VC columns by table name for v2 schema field creation"""
        table_columns = {}

        for table in vc_tables:
            table_name = table.get(c.NAME)
            if not table_name:
                continue

            # Add this line:
            clean_table_name_val = clean_table_name(table_name)
            columns = table.get(c.COLUMNS, [])

            # Use cleaned name as key:
            if clean_table_name_val not in table_columns:
                table_columns[clean_table_name_val] = {
                    "original_name": table_name,
                    "columns": [],
                }

            table_columns[clean_table_name_val]["columns"].extend(columns)

        return table_columns

    def _create_vc_upstream_lineage_v2(
        self, vc: dict, vc_tables: List[dict], vc_urn: str
    ) -> Tuple[List[Upstream], List[FineGrainedLineage]]:
        """Create upstream lineage for VC using v2 field paths"""
        upstream_tables = []
        fine_grained_lineages = []

        logger.info(f"Processing VC upstream lineage for {len(vc_tables)} tables")

        for vc_table in vc_tables:
            vc_table_name = vc_table.get(c.NAME)
            if not vc_table_name:
                logger.warning("VC table has no name, skipping")
                continue

            logger.info(f"Processing VC table: {vc_table_name}")

            # Find matching database table
            matched_db_table = self.tableau_source._find_matching_database_table(
                vc_table_name
            )
            if not matched_db_table:
                logger.warning(
                    f"No matching database table found for VC table: {vc_table_name}"
                )
                continue

            logger.info(f"Found matching database table for {vc_table_name}")

            # Create database table URN
            db_table_urn = self.tableau_source._create_database_table_urn(
                matched_db_table
            )
            if not db_table_urn:
                logger.warning(
                    f"Failed to create URN for matched database table: {matched_db_table.get('name', 'Unknown')}"
                )
                continue

            logger.info(f"Created database table URN: {db_table_urn}")

            # Add table-level upstream
            upstream_tables.append(
                Upstream(dataset=db_table_urn, type=DatasetLineageType.TRANSFORMED)
            )

            # Create column-level lineage using v2 field paths
            if self.config.extract_column_level_lineage:
                vc_columns = vc_table.get(c.COLUMNS, [])
                db_columns = matched_db_table.get(c.COLUMNS, [])

                if vc_columns and db_columns:
                    # Create mapping of database column names (case-insensitive)
                    db_column_map = {
                        col.get(c.NAME, "").lower(): col.get(c.NAME, "")
                        for col in db_columns
                        if col.get(c.NAME)
                    }

                    for vc_column in vc_columns:
                        vc_col_name = vc_column.get(c.NAME)
                        if not vc_col_name:
                            continue

                        vc_col_type = vc_column.get(c.REMOTE_TYPE, c.UNKNOWN)

                        # Check if this VC column matches a database column
                        if vc_col_name.lower() in db_column_map:
                            db_col_name = db_column_map[vc_col_name.lower()]

                            # Create v2 field path for VC column
                            clean_vc_table_name = clean_table_name(vc_table_name)
                            clean_vc_col_name = clean_table_name(vc_col_name)
                            vc_field_path = f"[version=2.0].[type=struct].[type=struct].{clean_vc_table_name}.[type={vc_col_type.lower()}].{clean_vc_col_name}"

                            # Create fine-grained lineage
                            fine_grained_lineages.append(
                                FineGrainedLineage(
                                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                                    downstreams=[
                                        builder.make_schema_field_urn(
                                            vc_urn, vc_field_path
                                        )
                                    ],
                                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                                    upstreams=[
                                        builder.make_schema_field_urn(
                                            db_table_urn, db_col_name
                                        )
                                    ],
                                )
                            )

        logger.info(
            f"Created {len(upstream_tables)} upstream table relationships for VC"
        )
        return upstream_tables, fine_grained_lineages

    def create_datasource_vc_lineage_v2(
        self, datasource_urn: str
    ) -> Tuple[List[Upstream], List[FineGrainedLineage]]:
        """Create datasource to VC lineage using v2 field paths - FIXED to return both table and column lineage"""
        upstream_tables: List[Upstream] = []
        fine_grained_lineages: List[FineGrainedLineage] = []

        # Extract datasource ID from URN
        try:
            # URN format: urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_id,PROD)
            urn_parts = datasource_urn.split(",")
            if len(urn_parts) >= 2:
                datasource_id = urn_parts[1]
            else:
                logger.warning(
                    f"Could not extract datasource ID from URN: {datasource_urn}"
                )
                return upstream_tables, fine_grained_lineages
        except Exception as e:
            logger.warning(
                f"Error extracting datasource ID from URN {datasource_urn}: {e}"
            )
            return upstream_tables, fine_grained_lineages

        if datasource_id not in self.datasource_vc_relationships:
            return upstream_tables, fine_grained_lineages

        vc_references = self.datasource_vc_relationships[datasource_id]
        logger.debug(
            f"Creating VC lineage for datasource {datasource_id} with {len(vc_references)} VC references"
        )

        # Track unique VCs for table-level lineage (avoid duplicates)
        vc_ids_seen = set()

        for ref in vc_references:
            vc_table_id = ref.get("vc_table_id")
            field_name = str(ref.get("field_name"))
            column_name = ref.get("column_name")

            if not all([vc_table_id, field_name, column_name]):
                continue

            if (
                vc_table_id in self.vc_table_id_to_vc_id
                and vc_table_id in self.vc_table_id_to_name
            ):
                vc_id = self.vc_table_id_to_vc_id[vc_table_id]
                table_name = self.vc_table_id_to_name[vc_table_id]

                vc_urn = builder.make_dataset_urn_with_platform_instance(
                    self.platform, vc_id, self.config.platform_instance, self.config.env
                )

                if vc_id not in vc_ids_seen:
                    vc_ids_seen.add(vc_id)
                    upstream_tables.append(
                        Upstream(dataset=vc_urn, type=DatasetLineageType.TRANSFORMED)
                    )
                    logger.debug(f"Added table-level upstream: {vc_urn}")

                # Add column-level lineage with v2 field paths
                if self.config.extract_column_level_lineage:
                    # Get column type from stored mappings
                    column_type = self.vc_table_column_types.get(
                        f"{vc_table_id}.{column_name}", c.UNKNOWN
                    )

                    # Create v2 field path for VC column
                    clean_table_name_val = clean_table_name(table_name)
                    clean_column_name_val = clean_table_name(str(column_name))
                    vc_field_path = f"[version=2.0].[type=struct].[type=struct].{clean_table_name_val}.[type={column_type.lower()}].{clean_column_name_val}"

                    fine_grained_lineages.append(
                        FineGrainedLineage(
                            downstreamType=FineGrainedLineageDownstreamType.FIELD,
                            downstreams=[
                                builder.make_schema_field_urn(
                                    datasource_urn, field_name
                                )
                            ],
                            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                            upstreams=[
                                builder.make_schema_field_urn(vc_urn, vc_field_path)
                            ],
                        )
                    )

                    logger.debug(f"Created VC lineage: {field_name} ← {vc_field_path}")

        logger.debug(
            f"Created {len(upstream_tables)} table lineages and {len(fine_grained_lineages)} column lineages for datasource {datasource_id}"
        )
        return upstream_tables, fine_grained_lineages

    def emit_datasource_vc_lineages(self) -> Iterable[MetadataWorkUnit]:
        """Emit datasource → VC lineage relationships"""
        if not self.datasource_vc_relationships or not self.vc_table_id_to_vc_id:
            logger.info("No datasource VC relationships to emit")
            return

        for datasource_id in self.datasource_vc_relationships:
            datasource_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=datasource_id,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            # Get both table and column lineage
            upstream_tables, fine_grained_lineages = (
                self.create_datasource_vc_lineage_v2(datasource_urn)
            )

            if upstream_tables or fine_grained_lineages:
                upstream_lineage = UpstreamLineage(
                    upstreams=upstream_tables,
                    fineGrainedLineages=fine_grained_lineages or None,
                )
                yield self.tableau_source.get_metadata_change_proposal(
                    datasource_urn,
                    aspect_name=c.UPSTREAM_LINEAGE,
                    aspect=upstream_lineage,
                )

    def _get_vc_project_luid(self, vc: dict) -> Optional[str]:
        """Get project LUID for a Virtual Connection"""
        # VCs should have project information similar to published datasources
        project_name = vc.get("projectName")
        if project_name:
            # Find project by name in the project registry
            for (
                project_id,
                project,
            ) in self.tableau_source.tableau_project_registry.items():
                if project.name == project_name:
                    return project_id
        return None
