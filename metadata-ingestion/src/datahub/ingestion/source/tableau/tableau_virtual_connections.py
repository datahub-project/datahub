import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_entity_to_container,
    gen_containers,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.tableau import tableau_constant as c
from datahub.ingestion.source.tableau.tableau_common import (
    FIELD_TYPE_MAPPING,
    DatasourceType,
    LineageResult,
    is_table_name_field,
    virtual_connection_detailed_graphql_query,
    virtual_connection_graphql_query,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    NullTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn

logger = logging.getLogger(__name__)

# Pre-compiled regex for extracting table references from field names like "column_name (schema.table_name)"
FIELD_TABLE_REFERENCE_PATTERN = re.compile(r"(.*?)\s*\((.*?)\)")


class VCFolderKey(ContainerKey):
    """Container key for Virtual Connection folders"""

    virtual_connection_id: str


class VirtualConnectionProcessor:
    """Handles Virtual Connection processing for Tableau connector"""

    def __init__(self, tableau_source):
        self.tableau_source = tableau_source
        self.config = tableau_source.config
        self.report = tableau_source.report
        self.server = tableau_source.server
        self.platform = tableau_source.platform
        self.ctx = tableau_source.ctx

        # Virtual Connection tracking data structures
        self.vc_table_ids_for_lookup: set = set()
        self.vc_table_id_to_vc_id: Dict[str, str] = {}
        self.vc_table_id_to_name: Dict[str, str] = {}
        self.virtual_connection_ids_being_used: List[str] = []
        self.datasource_vc_relationships: Dict[str, List[Dict[str, Any]]] = {}
        self.vc_table_column_types: Dict[str, str] = {}

    def gen_vc_folder_key(self, vc_id: str) -> VCFolderKey:
        return VCFolderKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            virtual_connection_id=vc_id,
        )

    def _extract_vc_references_from_field(
        self,
        field: dict,
        field_name: str,
        datasource_type: DatasourceType,
        datasource_name: str,
    ) -> List[Dict[str, Any]]:
        """Extract Virtual Connection references from a single field's upstream columns."""
        vc_references: List[Dict[str, Any]] = []

        upstream_columns = field.get(c.UPSTREAM_COLUMNS, [])
        if not isinstance(upstream_columns, list):
            logger.warning(
                f"Expected list for upstream_columns in field '{field_name}', got {type(upstream_columns)}. Skipping."
            )
            return vc_references

        for upstream_col in upstream_columns:
            if not isinstance(upstream_col, dict):
                logger.debug(
                    f"Skipping non-dict upstream column in field '{field_name}'"
                )
                continue

            table = upstream_col.get(c.TABLE, {})
            if not isinstance(table, dict):
                logger.debug(
                    f"Skipping upstream column with non-dict table in field '{field_name}'"
                )
                continue

            table_type = table.get(c.TYPE_NAME)

            if table_type == c.VIRTUAL_CONNECTION_TABLE:
                vc_table_id = table.get(c.ID)
                vc_table_name = table.get(c.NAME)
                column_name = upstream_col.get(c.NAME)

                if not vc_table_id:
                    logger.warning(
                        f"VC table missing required 'id' field in field '{field_name}'. Skipping."
                    )
                    continue
                if not vc_table_name:
                    logger.warning(
                        f"VC table missing required 'name' field (id={vc_table_id}) in field '{field_name}'. Skipping."
                    )
                    continue

                vc_info = table.get("virtualConnection", {})
                vc_id = (
                    vc_info.get(c.ID) if vc_info and isinstance(vc_info, dict) else None
                )

                # Validate that this is a proper column mapping, not a table-level reference
                if not column_name or column_name == vc_table_name:
                    logger.debug(
                        f"Skipping invalid Virtual Connection reference: field={field_name}, column={column_name}, table={vc_table_name}"
                    )
                    continue

                logger.debug(
                    f"Found VC reference in {datasource_type} datasource '{datasource_name}': "
                    f"field={field_name}, vc_table={vc_table_name}, column={column_name}, "
                    f"vc_table_id={vc_table_id}, vc_id={vc_id}"
                )

                # Store raw table name to handle matching issues like "TABLE_NAME (SCHEMA.TABLE_NAME)"
                raw_table_name = table.get(
                    c.NAME
                )  # This is the exact name from Tableau

                vc_references.append(
                    {
                        "field_name": field_name,
                        "vc_table_id": vc_table_id,
                        "vc_table_name": vc_table_name,
                        "raw_table_name": raw_table_name,  # Store the raw name for better matching
                        "column_name": column_name,
                        "vc_id": vc_id,
                    }
                )

                # Also store the raw table name for matching purposes
                if raw_table_name and raw_table_name != vc_table_name:
                    logger.debug(
                        f"  Raw table name differs from VC table name: '{raw_table_name}' vs '{vc_table_name}'"
                    )

                if vc_table_id:
                    self.vc_table_ids_for_lookup.add(vc_table_id)

        return vc_references

    def process_datasource_for_vc_refs(
        self, datasource: dict, datasource_type: DatasourceType
    ) -> None:
        """Process a single datasource for VC references - called during datasource emission"""
        datasource_id = datasource.get(c.ID)
        datasource_name = datasource.get(c.NAME, "Unknown")
        if not datasource_id:
            return

        logger.debug(
            f"Processing {datasource_type} datasource for VC references: "
            f"ID={datasource_id}, Name={datasource_name}"
        )

        vc_references = []

        fields = datasource.get(c.FIELDS, [])
        for field in fields:
            field_name = field.get(c.NAME)
            if not field_name:
                continue

            # Skip table name fields (e.g., "TABLE_NAME (SCHEMA.TABLE_NAME)") - not real columns
            field_type = field.get(c.TYPE_NAME, "")
            if is_table_name_field(field_name, field_type):
                logger.debug(
                    f"Skipping field '{field_name}' as it appears to be a table name reference, not a column field"
                )
                continue

            vc_refs_from_field = self._extract_vc_references_from_field(
                field, field_name, datasource_type, datasource_name
            )
            vc_references.extend(vc_refs_from_field)

        if vc_references:
            self.datasource_vc_relationships[datasource_id] = vc_references
            self.report.num_vc_table_references_found += len(vc_references)
            logger.info(
                f"Found {len(vc_references)} Virtual Connection references in {datasource_type} "
                f"datasource '{datasource_name}'"
            )
        else:
            logger.debug(
                f"No VC references found in {datasource_type} datasource '{datasource_name}' (ID: {datasource_id})"
            )

    def lookup_vc_ids_from_table_ids(self) -> None:
        """Step 2: Lookup VC IDs from VC table IDs and store mappings"""
        if not self.vc_table_ids_for_lookup:
            logger.debug(
                "No VC table IDs to lookup - no Virtual Connection references found"
            )
            return

        logger.info(
            f"Looking up {len(self.vc_table_ids_for_lookup)} VC table IDs for Virtual Connection processing"
        )
        logger.debug(f"VC table IDs: {self.vc_table_ids_for_lookup}")

        # Query all VCs to find matches
        for vc in self.tableau_source.get_connection_objects(
            query=virtual_connection_graphql_query,
            connection_type=c.VIRTUAL_CONNECTIONS_CONNECTION,
            query_filter={},  # Get all VCs
            page_size=self.config.effective_virtual_connection_page_size,
        ):
            if not isinstance(vc, dict):
                logger.warning(
                    f"Expected dict for VC object, got {type(vc)}. Skipping."
                )
                continue

            vc_id = vc.get(c.ID)
            if not vc_id:
                logger.warning(
                    "Virtual Connection missing required 'id' field. Skipping."
                )
                continue

            vc_name = vc.get(c.NAME, "Unknown")
            tables = vc.get("tables", [])
            if not isinstance(tables, list):
                logger.warning(
                    f"Expected list for tables in VC '{vc_name}' (id={vc_id}), got {type(tables)}. Skipping."
                )
                continue

            for table in tables:
                if not isinstance(table, dict):
                    logger.debug(f"Skipping non-dict table in VC '{vc_name}'")
                    continue

                table_id = table.get(c.ID)
                table_name = table.get(c.NAME)

                if not table_id or not table_name:
                    logger.debug(
                        f"Skipping table with missing id or name in VC '{vc_name}'"
                    )
                    continue

                if table_id in self.vc_table_ids_for_lookup:
                    logger.debug(
                        f"Found VC table match: {table_name} (ID: {table_id}) in VC: {vc_name}"
                    )

                    self.vc_table_id_to_vc_id[table_id] = vc_id
                    self.vc_table_id_to_name[table_id] = table_name

                    if vc_id not in self.virtual_connection_ids_being_used:
                        self.virtual_connection_ids_being_used.append(vc_id)

                    columns = table.get(c.COLUMNS, [])
                    for column in columns:
                        col_name = column.get(c.NAME)
                        col_type = column.get(c.REMOTE_TYPE, c.UNKNOWN)
                        if col_name:
                            self.vc_table_column_types[f"{table_id}.{col_name}"] = (
                                col_type
                            )

        self.report.num_virtual_connections_processed = len(
            self.virtual_connection_ids_being_used
        )
        logger.info(
            f"VC Lookup Results: Found {len(self.vc_table_id_to_vc_id)} VC table mappings, "
            f"will process {len(self.virtual_connection_ids_being_used)} Virtual Connections"
        )

        for table_id, vc_id in self.vc_table_id_to_vc_id.items():
            table_name = self.vc_table_id_to_name.get(table_id, "Unknown")
            logger.debug(
                f"  VC Table Mapping: {table_name} (ID: {table_id}) -> VC: {vc_id}"
            )

        if not self.vc_table_id_to_vc_id:
            logger.info(
                "No VC table mappings found - no Virtual Connection tables are being used by datasources."
            )

    def emit_virtual_connections(self):
        """Emit Virtual Connection datasets with schema fields"""
        if not self.virtual_connection_ids_being_used:
            logger.info("No Virtual Connections to emit")
            return

        logger.info(
            f"Emitting {len(self.virtual_connection_ids_being_used)} Virtual Connections"
        )

        vc_filter = {c.ID_WITH_IN: self.virtual_connection_ids_being_used}

        for vc in self.tableau_source.get_connection_objects(
            query=virtual_connection_detailed_graphql_query,
            connection_type=c.VIRTUAL_CONNECTIONS_CONNECTION,
            query_filter=vc_filter,
            page_size=self.config.effective_virtual_connection_page_size,
        ):
            yield from self._emit_single_virtual_connection(vc)

    def _group_vc_columns_by_table(
        self, vc_tables: List[dict]
    ) -> Dict[str, Dict[str, Any]]:
        """Group VC columns by table name"""
        table_columns = {}

        for table in vc_tables:
            table_name = table.get(c.NAME)
            if not table_name:
                continue

            columns = table.get(c.COLUMNS, [])
            if table_name not in table_columns:
                table_columns[table_name] = {
                    "original_name": table_name,
                    "columns": [],
                }

            table_columns[table_name]["columns"].extend(columns)

        return table_columns

    def _get_vc_schema_metadata_grouped_by_table(
        self, vc_tables: List[dict]
    ) -> Dict[str, SchemaMetadataClass]:
        """Create separate schema metadata for each table in the Virtual Connection"""
        table_schemas = {}
        table_columns = self._group_vc_columns_by_table(vc_tables)

        for table_name, table_info in table_columns.items():
            fields = []
            columns = table_info["columns"]

            for column in columns:
                column_name = column.get(c.NAME)
                if not column_name:
                    self.report.num_datasource_field_skipped_no_name += 1
                    logger.warning(
                        f"Skipping VC column {column.get(c.ID)} from schema since its name is none"
                    )
                    continue

                nativeDataType = column.get(c.REMOTE_TYPE, c.UNKNOWN)
                TypeClass = FIELD_TYPE_MAPPING.get(nativeDataType, NullTypeClass)

                schema_field = SchemaFieldClass(
                    fieldPath=column_name,
                    type=SchemaFieldDataTypeClass(type=TypeClass()),
                    description=column.get(c.DESCRIPTION),
                    nativeDataType=nativeDataType,
                )
                if schema_field:
                    fields.append(schema_field)

            if fields:
                table_schemas[table_name] = SchemaMetadataClass(
                    schemaName=f"VirtualConnection_{table_name}",
                    platform=f"urn:li:dataPlatform:{self.platform}",
                    version=0,
                    fields=fields,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                )

        return table_schemas

    def _create_vc_upstream_lineage(
        self, vc: dict, vc_tables: List[dict], vc_urn: str
    ) -> LineageResult:
        """Create upstream lineage for VC tables to their underlying database tables"""
        upstream_tables = []
        fine_grained_lineages = []

        logger.debug(f"Processing VC upstream lineage for {len(vc_tables)} tables")

        for vc_table in vc_tables:
            vc_table_name = vc_table.get(c.NAME)
            if not vc_table_name:
                logger.warning("VC table has no name, skipping")
                continue

            logger.debug(f"Processing VC table: {vc_table_name}")

            matched_db_table = self.tableau_source._find_matching_database_table(
                vc_table_name
            )
            if not matched_db_table:
                logger.warning(
                    f"No matching database table found for VC table: {vc_table_name}"
                )
                continue

            logger.debug(f"Found matching database table for {vc_table_name}")

            db_table_urn = self.tableau_source._create_database_table_urn(
                matched_db_table
            )
            if not db_table_urn:
                logger.warning(
                    f"Failed to create URN for matched database table: {matched_db_table.get('name', 'Unknown')}"
                )
                continue

            logger.debug(f"Created database table URN: {db_table_urn}")

            upstream_tables.append(
                UpstreamClass(
                    dataset=db_table_urn, type=DatasetLineageTypeClass.TRANSFORMED
                )
            )

            if self.config.extract_column_level_lineage:
                vc_columns = vc_table.get(c.COLUMNS, [])
                db_columns = matched_db_table.get(c.COLUMNS, [])

                logger.debug(
                    f"Creating column lineage for VC table '{vc_table_name}': "
                    f"{len(vc_columns)} VC columns, {len(db_columns)} DB columns"
                )

                if vc_columns and db_columns:
                    # Case-insensitive mapping for column matching
                    db_column_map = {
                        col.get(c.NAME, "").lower(): col.get(c.NAME, "")
                        for col in db_columns
                        if col.get(c.NAME)
                    }

                    for vc_column in vc_columns:
                        vc_col_name = vc_column.get(c.NAME)
                        if not vc_col_name:
                            continue

                        vc_column.get(c.REMOTE_TYPE, c.UNKNOWN)

                        if vc_col_name.lower() in db_column_map:
                            db_col_name = db_column_map[vc_col_name.lower()]

                            logger.debug(
                                f"Creating column lineage: VC column '{vc_col_name}' -> DB column '{db_col_name}'"
                            )

                            final_db_col_name = db_col_name
                            if (
                                self.tableau_source.is_snowflake_urn(db_table_urn)
                                and not self.config.ingest_tables_external
                            ):
                                # Snowflake normalizes field names - match that behavior for lineage
                                final_db_col_name = self.tableau_source._normalize_snowflake_column_name(
                                    db_col_name
                                )
                                logger.debug(
                                    f"Applied Snowflake normalization: '{db_col_name}' -> '{final_db_col_name}'"
                                )

                            fine_grained_lineages.append(
                                FineGrainedLineageClass(
                                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                    downstreams=[
                                        builder.make_schema_field_urn(
                                            vc_urn, vc_col_name
                                        )
                                    ],
                                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                    upstreams=[
                                        builder.make_schema_field_urn(
                                            db_table_urn, final_db_col_name
                                        )
                                    ],
                                )
                            )
                        else:
                            logger.debug(
                                f"No matching DB column found for VC column '{vc_col_name}'"
                            )
                else:
                    logger.debug(
                        f"Skipping column lineage for VC table '{vc_table_name}': "
                        f"VC columns={len(vc_columns)}, DB columns={len(db_columns)}"
                    )

        logger.debug(
            f"Created {len(upstream_tables)} upstream table relationships for VC"
        )
        return LineageResult(
            upstream_tables=upstream_tables, fine_grained_lineages=fine_grained_lineages
        )

    def create_datasource_vc_lineage(self, datasource_urn: str) -> LineageResult:
        """Create datasource to Virtual Connection column-level lineage.

        This method creates fine-grained (column-level) lineage between Tableau datasources
        and Virtual Connection tables. It processes field-to-column mappings that were
        collected during the datasource emission phase.

        Note: Table-level lineage (datasource -> VC table) is handled by get_upstream_vc_tables()
        in the main Tableau ingestion flow and merged with the datasource's upstream lineage.
        This method only returns fine_grained_lineages; upstream_tables is always empty here.

        Args:
            datasource_urn: URN of the Tableau datasource to create lineage for

        Returns:
            LineageResult with:
                - upstream_tables: Empty list (table-level lineage handled separately)
                - fine_grained_lineages: List of column-level lineage relationships
        """
        upstream_tables: List[UpstreamClass] = []
        fine_grained_lineages: List[FineGrainedLineageClass] = []

        logger.debug(f"Creating VC lineage for datasource URN: {datasource_urn}")
        logger.debug(
            f"Available datasource VC relationships: {list(self.datasource_vc_relationships.keys())}"
        )

        try:
            # URN format: urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_id,PROD)
            dataset_urn = DatasetUrn.from_string(datasource_urn)
            datasource_id = dataset_urn.name
            logger.debug(f"Extracted datasource ID: {datasource_id}")
        except Exception as e:
            logger.warning(
                f"Error parsing datasource URN {datasource_urn}: {e}. "
                "URN format should be: urn:li:dataset:(urn:li:dataPlatform:tableau,datasource_id,PROD)"
            )
            return LineageResult(
                upstream_tables=upstream_tables,
                fine_grained_lineages=fine_grained_lineages,
            )

        if datasource_id not in self.datasource_vc_relationships:
            logger.debug(
                f"No VC relationships found for datasource ID: {datasource_id}"
            )
            return LineageResult(
                upstream_tables=upstream_tables,
                fine_grained_lineages=fine_grained_lineages,
            )

        vc_references = self.datasource_vc_relationships[datasource_id]
        logger.debug(
            f"Creating VC lineage for datasource {datasource_id} with {len(vc_references)} VC references"
        )

        vc_table_urns_seen = set()

        # Build mapping of VC table names to IDs for disambiguation when field names
        # reference specific tables (e.g., "column_name (schema.table_name)")
        vc_table_name_to_id: Dict[str, str] = {}
        for table_id, _vc_id in self.vc_table_id_to_vc_id.items():
            if table_id in self.vc_table_id_to_name:
                table_name = self.vc_table_id_to_name[table_id]
                vc_table_name_to_id[table_name.lower()] = table_id

        for ref in vc_references:
            vc_table_id = ref.get("vc_table_id")
            field_name = str(ref.get("field_name"))
            vc_table_name = ref.get("vc_table_name")
            column_name = ref.get("column_name")

            if not all([vc_table_id, field_name, column_name, vc_table_name]):
                continue

            # Extract table reference from field names like "column_name (schema.table_name)"
            # Helps disambiguate which VC table a field comes from when VCs have multiple tables
            referenced_table = None
            clean_field_name = field_name
            table_match = FIELD_TABLE_REFERENCE_PATTERN.search(field_name)
            if table_match:
                clean_field_name = table_match.group(1).strip()
                referenced_table = table_match.group(2).strip().lower()
                logger.debug(
                    f"Extracted referenced table '{referenced_table}' from field '{field_name}'"
                )

                if referenced_table in vc_table_name_to_id:
                    ref_table_id = vc_table_name_to_id[referenced_table]
                    if ref_table_id != vc_table_id:
                        logger.debug(
                            f"Overriding table ID from {vc_table_id} to {ref_table_id} based on field name reference"
                        )
                        vc_table_id = ref_table_id
                        if ref_table_id in self.vc_table_id_to_name:
                            vc_table_name = self.vc_table_id_to_name[ref_table_id]

            if vc_table_id in self.vc_table_id_to_vc_id:
                vc_id = self.vc_table_id_to_vc_id[vc_table_id]

                # URN format: {vc_id}.{table_name}
                vc_table_urn = builder.make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=f"{vc_id}.{vc_table_name}",
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )

                # Table-level lineage handled elsewhere; just track URNs here
                if vc_table_urn not in vc_table_urns_seen:
                    vc_table_urns_seen.add(vc_table_urn)

                if self.config.extract_column_level_lineage:
                    self.vc_table_column_types.get(
                        f"{vc_table_id}.{column_name}", c.UNKNOWN
                    )

                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            downstreams=[
                                builder.make_schema_field_urn(
                                    datasource_urn, field_name
                                )
                            ],
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                builder.make_schema_field_urn(
                                    vc_table_urn, str(column_name)
                                )
                            ],
                            transformOperation=f"Source: {clean_field_name} from {vc_table_name}"
                            if referenced_table
                            else None,
                        )
                    )
                    self.report.num_vc_lineages_created += 1

                    logger.debug(
                        f"Created VC lineage: {field_name} ← {column_name} (in {vc_table_name})"
                    )

        logger.debug(
            f"Created {len(upstream_tables)} table lineages and {len(fine_grained_lineages)} column lineages for datasource {datasource_id}"
        )
        return LineageResult(
            upstream_tables=upstream_tables, fine_grained_lineages=fine_grained_lineages
        )

    def _get_vc_project_luid(self, vc: dict) -> Optional[str]:
        """Get project LUID for a Virtual Connection"""
        project_name = vc.get("projectName")
        if project_name:
            for (
                project_id,
                project,
            ) in self.tableau_source.tableau_project_registry.items():
                if project.name == project_name:
                    return project_id
        return None

    def _create_vc_folder_container(
        self, vc: dict
    ) -> Tuple[str, List[MetadataWorkUnit]]:
        """Create a folder container for a Virtual Connection"""
        vc_id = vc.get(c.ID)
        vc_name = vc.get(c.NAME, "Unknown Virtual Connection")
        vc_description = vc.get(c.DESCRIPTION, "")

        if not vc_id:
            raise ValueError("VC ID is required for container creation")

        vc_folder_key = VCFolderKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            virtual_connection_id=vc_id,
        )

        container_urn = vc_folder_key.as_urn()
        container_workunits = list(
            gen_containers(
                container_key=vc_folder_key,
                name=vc_name,
                description=vc_description,
                sub_types=["Virtual Connection"],
            )
        )

        project_luid = self._get_vc_project_luid(vc)
        if project_luid:
            project_key = self.tableau_source.gen_project_key(project_luid)
            container_workunits.extend(
                add_entity_to_container(project_key, "container", container_urn)
            )

        return container_urn, container_workunits

    def _emit_single_virtual_connection(self, vc: dict) -> Iterable[MetadataWorkUnit]:
        """Emit a single Virtual Connection dataset"""
        vc_id = vc[c.ID]
        vc.get(c.NAME, "Unknown Virtual Connection")

        vc_container_urn, container_workunits = self._create_vc_folder_container(vc)
        yield from container_workunits

        vc_tables = vc.get("tables", [])
        table_schemas = self._get_vc_schema_metadata_grouped_by_table(vc_tables)

        for table_name, schema_metadata in table_schemas.items():
            table_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=f"{vc_id}.{table_name}",
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            dataset_snapshot = DatasetSnapshotClass(
                urn=table_urn,
                aspects=[self.tableau_source.get_data_platform_instance()],
            )

            table_info: dict = next(
                (t for t in vc_tables if t.get(c.NAME) == table_name),
                {},
            )
            dataset_props = DatasetPropertiesClass(
                name=table_info.get(c.NAME, table_name),
                description=table_info.get(c.DESCRIPTION),
                customProperties=self.tableau_source.get_custom_props_from_dict(
                    table_info, [c.LUID]
                ),
            )
            dataset_snapshot.aspects.append(dataset_props)
            dataset_snapshot.aspects.append(schema_metadata)

            lineage_result = self._create_table_upstream_lineage(table_info, table_urn)

            if lineage_result.upstream_tables or lineage_result.fine_grained_lineages:
                logger.debug(
                    f"Emitting upstream lineage for VC table '{table_name}': "
                    f"{len(lineage_result.upstream_tables)} upstream tables, {len(lineage_result.fine_grained_lineages)} fine-grained lineages"
                )
                upstream_lineage = UpstreamLineageClass(
                    upstreams=lineage_result.upstream_tables,
                    fineGrainedLineages=lineage_result.fine_grained_lineages or None,
                )
                yield self.tableau_source.get_metadata_change_proposal(
                    table_urn,
                    upstream_lineage,
                )
            else:
                logger.debug(f"No upstream lineage to emit for VC table '{table_name}'")

            vc_folder_key = self.gen_vc_folder_key(vc_id)
            yield from add_entity_to_container(
                vc_folder_key,
                c.DATASET,
                table_urn,
            )

            yield self.tableau_source.get_metadata_change_event(dataset_snapshot)
            yield self.tableau_source.get_metadata_change_proposal(
                dataset_snapshot.urn,
                SubTypesClass(typeNames=["Virtual Connection Table"]),
            )

    def _create_table_upstream_lineage(
        self, table_info: dict, table_urn: str
    ) -> LineageResult:
        """Create upstream lineage for a single table"""
        upstream_tables = []
        fine_grained_lineages = []

        table_name = table_info.get(c.NAME)
        if not table_name:
            return LineageResult(upstream_tables=[], fine_grained_lineages=[])

        logger.debug(f"Creating upstream lineage for VC table: {table_name}")
        matched_db_table = self.tableau_source._find_matching_database_table(table_name)
        if not matched_db_table:
            logger.warning(
                f"No matching database table found for VC table: {table_name}"
            )
            return LineageResult(upstream_tables=[], fine_grained_lineages=[])

        logger.debug(
            f"Found matching database table for VC table '{table_name}': {matched_db_table.get('name', 'Unknown')}"
        )

        db_table_urn = self.tableau_source._create_database_table_urn(matched_db_table)
        if not db_table_urn:
            logger.warning(
                f"Failed to create URN for matched database table: {matched_db_table.get('name', 'Unknown')}"
            )
            return LineageResult(upstream_tables=[], fine_grained_lineages=[])

        upstream_tables.append(
            UpstreamClass(
                dataset=db_table_urn, type=DatasetLineageTypeClass.TRANSFORMED
            )
        )

        if self.config.extract_column_level_lineage:
            vc_columns = table_info.get(c.COLUMNS, [])
            db_columns = matched_db_table.get(c.COLUMNS, [])

            if vc_columns and db_columns:
                # Case-insensitive mapping for column matching
                db_column_map = {
                    col.get(c.NAME, "").lower(): col.get(c.NAME, "")
                    for col in db_columns
                    if col.get(c.NAME)
                }

                for vc_column in vc_columns:
                    vc_col_name = vc_column.get(c.NAME)
                    if not vc_col_name:
                        continue

                    if vc_col_name.lower() in db_column_map:
                        db_col_name = db_column_map[vc_col_name.lower()]

                        final_db_col_name = db_col_name
                        if (
                            self.tableau_source.is_snowflake_urn(db_table_urn)
                            and not self.config.ingest_tables_external
                        ):
                            # Snowflake normalizes field names - match that behavior for lineage
                            final_db_col_name = (
                                self.tableau_source._normalize_snowflake_column_name(
                                    db_col_name
                                )
                            )

                        fine_grained_lineages.append(
                            FineGrainedLineageClass(
                                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                downstreams=[
                                    builder.make_schema_field_urn(
                                        table_urn, vc_col_name
                                    )
                                ],
                                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                upstreams=[
                                    builder.make_schema_field_urn(
                                        db_table_urn, final_db_col_name
                                    )
                                ],
                            )
                        )

        return LineageResult(
            upstream_tables=upstream_tables, fine_grained_lineages=fine_grained_lineages
        )
