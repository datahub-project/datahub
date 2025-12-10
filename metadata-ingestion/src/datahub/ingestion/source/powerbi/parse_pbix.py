import json
import logging
import re
import struct
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import xpress9

from datahub.ingestion.source.powerbi.models import (
    ColumnLineageEntry,
    ColumnPageMapping,
    ColumnUsageOnPage,
    DataRole,
    MeasureLineageEntry,
    MeasurePageMapping,
    MeasureUsageOnPage,
    PageUsageInfo,
    PBIXBookmark,
    PBIXColumn,
    PBIXColumnMapping,
    PBIXDataModel,
    PBIXDataModelParsed,
    PBIXDataSource,
    PBIXDataTransformSelect,
    PBIXExpression,
    PBIXExtractedMetadata,
    PBIXExtractResult,
    PBIXFileInfo,
    PBIXHierarchy,
    PBIXLayout,
    PBIXLayoutParsedEnhanced,
    PBIXLineageResult,
    PBIXMeasure,
    PBIXMeasureMapping,
    PBIXPageLineage,
    PBIXParameter,
    PBIXPartition,
    PBIXRelationship,
    PBIXRole,
    PBIXSection,
    PBIXSelectItem,
    PBIXTable,
    PBIXTableParsed,
    PBIXTableUsage,
    PBIXVisualContainer,
    SectionInfo,
    VisualColumnInfo,
    VisualInfo,
    VisualizationLineage,
    VisualizationReference,
    VisualizationUsage,
    VisualMeasureInfo,
    VisualPosition,
)

logger = logging.getLogger(__name__)


class LineageBuilder:
    """Helper class to build lineage using Pydantic models instead of dictionaries."""

    def __init__(self):
        self.visualization_lineage: List[VisualizationLineage] = []
        self.column_mapping: Dict[str, PBIXColumnMapping] = {}
        self.measure_mapping: Dict[str, PBIXMeasureMapping] = {}
        self.table_usage: Dict[str, PBIXTableUsage] = {}
        self.page_lineage: Dict[str, PBIXPageLineage] = {}
        self.page_mapping: Dict[str, Union[ColumnPageMapping, MeasurePageMapping]] = {}
        # Temporary sets for deduplication
        self._table_columns: Dict[str, Set[str]] = {}
        self._table_visualizations: Dict[str, List[VisualizationReference]] = {}

    def add_visualization_lineage(self, viz_lineage: VisualizationLineage) -> None:
        """Add a visualization lineage entry."""
        self.visualization_lineage.append(viz_lineage)

    def update_column_mapping(
        self,
        table: str,
        column: str,
        viz: VisualInfo,
        col: VisualColumnInfo,
    ) -> None:
        """Update column mapping with visualization usage."""
        if not (table and column):
            return

        key = f"{table}.{column}"
        if key not in self.column_mapping:
            self.column_mapping[key] = PBIXColumnMapping(
                table=table,
                column=column,
                used_in_visualizations=[],
            )

        viz_usage = VisualizationUsage(
            visualization_id=viz.id,
            visualization_type=viz.visualType,
            page=viz.sectionName,
            page_id=viz.sectionId,
            data_role=list(col.roles.keys()) if col.roles else [],
        )
        self.column_mapping[key].used_in_visualizations.append(viz_usage)

        # Update table usage
        if table not in self.table_usage:
            self.table_usage[table] = PBIXTableUsage(
                columns=[],
                visualizations=[],
            )
            self._table_columns[table] = set()
            self._table_visualizations[table] = []

        self._table_columns[table].add(column)

        viz_ref = VisualizationReference(
            id=viz.id,
            type=viz.visualType,
            page=viz.sectionName,
            page_id=viz.sectionId,
        )
        self._table_visualizations[table].append(viz_ref)

    def update_measure_mapping(
        self,
        entity: str,
        property_name: str,
        viz: VisualInfo,
        measure: VisualMeasureInfo,
    ) -> None:
        """Update measure mapping with visualization usage."""
        if not (entity and property_name):
            return

        key = f"{entity}.{property_name}"
        if key not in self.measure_mapping:
            self.measure_mapping[key] = PBIXMeasureMapping(
                entity=entity,
                measure=property_name,
                used_in_visualizations=[],
            )

        viz_usage = VisualizationUsage(
            visualization_id=viz.id,
            visualization_type=viz.visualType,
            page=viz.sectionName,
            page_id=viz.sectionId,
            data_role=list(measure.roles.keys()) if measure.roles else [],
        )
        self.measure_mapping[key].used_in_visualizations.append(viz_usage)

    def process_page_lineage_and_mapping(
        self, layout_parsed: PBIXLayoutParsedEnhanced
    ) -> None:
        """
        Process page-level lineage and build page mappings.

        Args:
            layout_parsed: Parsed layout with sections
        """
        for section in layout_parsed.sections:
            page_name = section.displayName
            page_id = section.id

            if page_name not in self.page_lineage:
                self.page_lineage[page_name] = PBIXPageLineage(
                    page_id=page_id,
                    page_name=page_name,
                    columns={},
                    measures={},
                    tables=[],
                    visualizations=[],
                )

            for viz in section.visualContainers:
                viz_type = viz.visualType if hasattr(viz, "visualType") else "Unknown"
                viz_id = viz.id if hasattr(viz, "id") else None

                viz_ref = VisualizationReference(
                    id=viz_id,
                    type=viz_type,
                )
                self.page_lineage[page_name].visualizations.append(viz_ref)

                # Process columns
                columns = viz.columns if hasattr(viz, "columns") else []
                for col in columns:
                    table = col.table if hasattr(col, "table") else ""
                    column = col.column if hasattr(col, "column") else ""
                    if table and column:
                        col_key = f"{table}.{column}"
                        if col_key not in self.page_lineage[page_name].columns:
                            self.page_lineage[page_name].columns[col_key] = (
                                ColumnUsageOnPage(
                                    table=table,
                                    column=column,
                                    visualizations=[],
                                )
                            )

                        viz_usage = VisualizationUsage(
                            visualization_id=viz_id,
                            visualization_type=viz_type,
                            data_role=list(col.roles.keys())
                            if hasattr(col, "roles") and col.roles
                            else [],
                        )
                        self.page_lineage[page_name].columns[
                            col_key
                        ].visualizations.append(viz_usage)

                        # Add table to page's table list (will be sorted in finalize)
                        if table not in self.page_lineage[page_name].tables:
                            self.page_lineage[page_name].tables.append(table)

                # Process measures
                measures = viz.measures if hasattr(viz, "measures") else []
                for measure in measures:
                    entity = measure.entity if hasattr(measure, "entity") else ""
                    property_name = (
                        measure.property if hasattr(measure, "property") else ""
                    )
                    if entity and property_name:
                        measure_key = f"{entity}.{property_name}"
                        if measure_key not in self.page_lineage[page_name].measures:
                            self.page_lineage[page_name].measures[measure_key] = (
                                MeasureUsageOnPage(
                                    entity=entity,
                                    measure=property_name,
                                    visualizations=[],
                                )
                            )

                        viz_usage = VisualizationUsage(
                            visualization_id=viz_id,
                            visualization_type=viz_type,
                            data_role=list(measure.roles.keys())
                            if hasattr(measure, "roles") and measure.roles
                            else [],
                        )
                        self.page_lineage[page_name].measures[
                            measure_key
                        ].visualizations.append(viz_usage)

            # Build page mapping
            for col_key, col_usage in self.page_lineage[page_name].columns.items():
                if col_key not in self.page_mapping:
                    self.page_mapping[col_key] = ColumnPageMapping(
                        table=col_usage.table,
                        column=col_usage.column,
                        used_in_pages=[],
                    )

                page_usage = PageUsageInfo(
                    page=page_name,
                    page_id=page_id,
                    visualization_count=len(col_usage.visualizations),
                )
                self.page_mapping[col_key].used_in_pages.append(page_usage)

            for measure_key, measure_usage in self.page_lineage[
                page_name
            ].measures.items():
                if measure_key not in self.page_mapping:
                    self.page_mapping[measure_key] = MeasurePageMapping(
                        entity=measure_usage.entity,
                        measure=measure_usage.measure,
                        used_in_pages=[],
                    )

                page_usage = PageUsageInfo(
                    page=page_name,
                    page_id=page_id,
                    visualization_count=len(measure_usage.visualizations),
                )
                self.page_mapping[measure_key].used_in_pages.append(page_usage)

    def finalize(self) -> PBIXLineageResult:
        """Finalize lineage by deduplicating and converting to result."""
        # Finalize table usage
        for table in self.table_usage:
            self.table_usage[table].columns = sorted(list(self._table_columns[table]))
            # Deduplicate visualizations
            seen = set()
            unique_viz = []
            for viz in self._table_visualizations[table]:
                viz_key = (viz.id, viz.type, viz.page)
                if viz_key not in seen:
                    seen.add(viz_key)
                    unique_viz.append(viz)
            self.table_usage[table].visualizations = unique_viz

        # Sort page tables
        for page_name in self.page_lineage:
            self.page_lineage[page_name].tables = sorted(
                self.page_lineage[page_name].tables
            )

        return PBIXLineageResult(
            visualization_lineage=self.visualization_lineage,
            column_mapping=self.column_mapping,
            measure_mapping=self.measure_mapping,
            table_usage=self.table_usage,
            page_lineage=self.page_lineage,
            page_mapping=self.page_mapping,
        )


class PBIXParser:
    def __init__(self, pbix_path: str):
        """
        Initialize the parser with a .pbix file path.

        Args:
            pbix_path: Path to the .pbix file
        """
        self.pbix_path = Path(pbix_path)
        if not self.pbix_path.exists():
            raise FileNotFoundError(f"File not found: {pbix_path}")
        if not self.pbix_path.suffix.lower() == ".pbix":
            raise ValueError(f"File must have .pbix extension: {pbix_path}")

        self.metadata: Dict[str, Any] = {}
        self.data_model_schema = None
        self.layout = None
        self.version = None

    def extract_zip(self) -> PBIXExtractResult:
        """
        Extract and parse the .pbix ZIP archive.

        Returns:
            PBIXExtractResult with strongly-typed parsed metadata
        """
        file_info = PBIXFileInfo(
            filename=self.pbix_path.name,
            size_bytes=self.pbix_path.stat().st_size,
        )

        version: Optional[str] = None
        metadata: Optional[Union[Dict[str, Any], str]] = None
        data_model: Optional[Dict[str, Any]] = None
        layout: Optional[Dict[str, Any]] = None
        file_list: List[str] = []

        try:
            with zipfile.ZipFile(self.pbix_path, "r") as zip_ref:
                # List all files in the archive
                file_list = sorted(zip_ref.namelist())

                # Extract and parse key files
                for filename in file_list:
                    try:
                        if filename == "Version":
                            content = zip_ref.read(filename).decode("utf-8")
                            version = content.strip()

                        elif filename == "Metadata":
                            content = zip_ref.read(filename).decode("utf-8")
                            # Metadata might not be JSON, try to parse it
                            try:
                                metadata = json.loads(content)
                            except json.JSONDecodeError:
                                # Metadata might be in a different format, store as raw text
                                metadata = content

                        elif filename in ("DataModelSchema", "DataModel"):
                            raw_content = zip_ref.read(filename)
                            content = None

                            # Try to decode as text first
                            try:
                                content = raw_content.decode("utf-16-le")
                                # Successfully decoded as text
                                data_model = json.loads(content)
                                continue
                            except UnicodeDecodeError:
                                # Not UTF-16LE, try UTF-8
                                try:
                                    content = raw_content.decode("utf-8")
                                    data_model = json.loads(content)
                                    continue
                                except (UnicodeDecodeError, json.JSONDecodeError):
                                    # Binary content - might be XPress9 compressed
                                    pass

                            # If we got here, file is binary - try XPress9 decompression
                            logger.info(
                                f"{filename} appears to be XPress9 compressed, attempting decompression..."
                            )
                            try:
                                # XPress9 format: first 8 bytes contain the uncompressed size (little-endian int64)
                                if len(raw_content) < 8:
                                    logger.warning(
                                        f"{filename} is too small to be a valid XPress9 file"
                                    )
                                    continue

                                # Read the uncompressed size from the header
                                uncompressed_size = struct.unpack(
                                    "<Q", raw_content[:8]
                                )[0]
                                compressed_data = raw_content[8:]  # Skip the header

                                logger.debug(
                                    f"XPress9 header: uncompressed_size={uncompressed_size}, compressed_size={len(compressed_data)}"
                                )

                                # Decompress using xpress9
                                decompressor = xpress9.Xpress9()
                                decompressed_data = decompressor.decompress(
                                    compressed_data, uncompressed_size
                                )

                                # Decode and parse as JSON
                                content = decompressed_data.decode("utf-16-le")
                                data_model = json.loads(content)
                                logger.info(
                                    f"✓ Successfully decompressed {filename} using XPress9 ({len(raw_content)} → {len(decompressed_data)} bytes)"
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to decompress {filename} with XPress9: {e}"
                                )
                                logger.debug(
                                    f"Raw content preview (first 100 bytes): {raw_content[:100]!r}"
                                )
                                continue

                        elif filename in ("Layout", "Report/Layout"):
                            raw_content = zip_ref.read(filename)
                            # Try UTF-16LE first (common in .pbix files), then UTF-8
                            try:
                                content = raw_content.decode("utf-16-le")
                            except UnicodeDecodeError:
                                content = raw_content.decode("utf-8")
                            layout = json.loads(content)

                    except (UnicodeDecodeError, json.JSONDecodeError) as e:
                        # Some files might be binary or have encoding issues
                        logger.warning(f"Could not parse {filename}: {e}")
                        continue

        except zipfile.BadZipFile as e:
            raise ValueError(f"Invalid ZIP file: {self.pbix_path}") from e

        return PBIXExtractResult(
            file_info=file_info,
            version=version,
            metadata=metadata,
            data_model=data_model,
            layout=layout,
            file_list=file_list,
        )

    def parse_data_model(self, data_model: Dict[str, Any]) -> PBIXDataModelParsed:
        """
        Parse the DataModelSchema to extract tables, columns, relationships, etc.
        Uses Pydantic models for validation and type safety.

        Args:
            data_model: The parsed DataModelSchema JSON

        Returns:
            PBIXDataModelParsed with strongly-typed structured information
        """
        if not data_model:
            return PBIXDataModelParsed()

        try:
            # Extract model structure
            model = data_model.get("model", {})
            tables_data = model.get("tables", [])
            relationships_data = model.get("relationships", [])

            # Parse tables using Pydantic model_validate
            tables = []
            all_hierarchies = []
            for table_data in tables_data:
                try:
                    # Parse columns with validation
                    columns = [
                        PBIXColumn.model_validate(col)
                        for col in table_data.get("columns", [])
                    ]

                    # Parse measures with validation
                    measures = [
                        PBIXMeasure.model_validate(measure)
                        for measure in table_data.get("measures", [])
                    ]

                    # Parse partitions with validation
                    partitions = [
                        PBIXPartition.model_validate(partition)
                        for partition in table_data.get("partitions", [])
                    ]

                    # Parse hierarchies with validation
                    hierarchies = []
                    for hierarchy_data in table_data.get("hierarchies", []):
                        try:
                            hierarchy = PBIXHierarchy.model_validate(hierarchy_data)
                            hierarchies.append(hierarchy)
                            all_hierarchies.append(hierarchy)
                        except Exception as e:
                            logger.warning(
                                f"Failed to parse hierarchy in table {table_data.get('name')}: {e}"
                            )
                            continue

                    # Create table model with validation
                    table = PBIXTable.model_validate(
                        {
                            "name": table_data.get("name", "Unknown"),
                            "columns": columns,
                            "measures": measures,
                            "partitions": partitions,
                            "hierarchies": hierarchies,
                            "isHidden": table_data.get("isHidden", False),
                        }
                    )
                    tables.append(table)
                except Exception as e:
                    logger.warning(
                        f"Failed to parse table {table_data.get('name', 'Unknown')}: {e}"
                    )
                    continue

            # Parse relationships using Pydantic model_validate
            relationships = []
            for rel_data in relationships_data:
                try:
                    relationship = PBIXRelationship.model_validate(rel_data)
                    relationships.append(relationship)
                except Exception as e:
                    logger.warning(f"Failed to parse relationship: {e}")
                    continue

            # Parse roles (Row-Level Security)
            roles = []
            roles_data = model.get("roles", [])
            for role_data in roles_data:
                try:
                    role = PBIXRole.model_validate(role_data)
                    roles.append(role)
                except Exception as e:
                    logger.warning(f"Failed to parse role: {e}")
                    continue

            # Parse data sources
            data_sources = []
            data_sources_data = model.get("dataSources", [])
            for ds_data in data_sources_data:
                try:
                    data_source = PBIXDataSource.model_validate(ds_data)
                    data_sources.append(data_source)
                except Exception as e:
                    logger.warning(f"Failed to parse data source: {e}")
                    continue

            # Parse expressions (M-Queries)
            expressions = []
            expressions_data = model.get("expressions", [])
            for expr_data in expressions_data:
                try:
                    expression = PBIXExpression.model_validate(expr_data)
                    expressions.append(expression)
                except Exception as e:
                    logger.warning(f"Failed to parse expression: {e}")
                    continue

            # Create complete data model with validation
            data_model_obj = PBIXDataModel.model_validate(
                {
                    "tables": tables,
                    "relationships": relationships,
                    "roles": roles,
                    "dataSources": data_sources,
                    "expressions": expressions,
                    "cultures": data_model.get("culture", "en-US"),
                    "version": data_model.get("version", "Unknown"),
                }
            )

            # Convert to strongly-typed result
            parsed_tables = [
                PBIXTableParsed(
                    name=table.name,
                    columns=[col.model_dump() for col in table.columns],
                    measures=[measure.model_dump() for measure in table.measures],
                    partitions=[
                        partition.model_dump() for partition in table.partitions
                    ],
                    isHidden=table.isHidden,
                )
                for table in data_model_obj.tables
            ]

            return PBIXDataModelParsed(
                tables=parsed_tables,
                relationships=[
                    rel.model_dump() for rel in data_model_obj.relationships
                ],
                hierarchies=all_hierarchies,
                roles=data_model_obj.roles,
                dataSources=data_model_obj.dataSources,
                expressions=data_model_obj.expressions,
                cultures=data_model_obj.cultures,
                version=data_model_obj.version,
                validated_model=data_model_obj,
            )
        except Exception as e:
            logger.error(f"Failed to parse data model: {e}")
            # Fallback to basic structure
            return PBIXDataModelParsed(
                tables=[],
                relationships=[],
                cultures=data_model.get("culture", "en-US"),
                version=data_model.get("version", "Unknown"),
            )

    def _parse_table_column_from_names(
        self,
        name: str,
        native_ref: str,
        col_source_entity: str = "",
        col_prop: str = "",
    ) -> Tuple[str, str]:
        if col_source_entity and col_prop:
            return col_source_entity, col_prop
        elif "." in native_ref:
            parts = native_ref.split(".", 1)
            return (
                parts[0] if len(parts) > 1 else "",
                parts[1] if len(parts) > 1 else native_ref,
            )
        elif "." in name:
            # Handle "Sum(DAX_ORDER_DETAILS.Avg_Order_Value)" format
            match = re.search(r"\((\w+)\.(\w+)\)", name)
            if match:
                return match.group(1), match.group(2)
            else:
                parts = name.split(".", 1)
                return (
                    parts[0] if len(parts) > 1 else "",
                    parts[1] if len(parts) > 1 else name,
                )
        else:
            return col_source_entity or "", col_prop or name

    def _process_aggregation_select_item(
        self,
        select_item: Dict[str, Any],
        name: str,
        native_ref: str,
        visual_columns: List[VisualColumnInfo],
    ) -> None:
        agg = select_item.get("Aggregation", {})
        agg_func = agg.get("Function", 0)
        agg_expr = agg.get("Expression", {})

        if "Column" not in agg_expr:
            return

        # Check if we already have this column
        if any(c.name == name for c in visual_columns):
            return

        func_names = {
            0: "Sum",
            1: "Min",
            2: "Max",
            3: "Count",
            4: "Average",
            5: "DistinctCount",
        }

        # Extract table and column from the column expression
        col_expr = agg_expr.get("Column", {})
        col_prop = col_expr.get("Property", "")
        col_source = col_expr.get("Expression", {}).get("SourceRef", {})
        col_source_entity = col_source.get("Source", "")

        table, column = self._parse_table_column_from_names(
            name, native_ref, col_source_entity, col_prop
        )

        column_info = VisualColumnInfo(
            name=name,
            nativeReferenceName=native_ref,
            table=table,
            column=column,
            aggregation=func_names.get(agg_func, "Sum"),
        )
        visual_columns.append(column_info)

    def _process_select_items(
        self,
        select_items: List[Dict[str, Any]],
        visual_columns: List[VisualColumnInfo],
        visual_measures: List[VisualMeasureInfo],
    ) -> None:
        for select_item in select_items:
            try:
                # Validate with Pydantic
                item_model = PBIXSelectItem.model_validate(select_item)
                name = item_model.Name or ""
                native_ref = item_model.NativeReferenceName or ""

                if item_model.Measure:
                    if not any(m.name == name for m in visual_measures):
                        measure_info = VisualMeasureInfo(
                            name=name,
                            nativeReferenceName=native_ref,
                            entity=name.split(".")[0] if "." in name else "",
                            property=name.split(".")[1] if "." in name else name,
                        )
                        visual_measures.append(measure_info)
                elif item_model.Column:
                    if not any(c.name == name for c in visual_columns):
                        column_info = VisualColumnInfo(
                            name=name,
                            nativeReferenceName=native_ref,
                            table=name.split(".")[0] if "." in name else "",
                            column=name.split(".")[1] if "." in name else name,
                        )
                        visual_columns.append(column_info)
                elif item_model.Aggregation:
                    self._process_aggregation_select_item(
                        select_item, name, native_ref, visual_columns
                    )
                elif item_model.HierarchyLevel:
                    if not any(c.name == name for c in visual_columns):
                        hierarchy_info = VisualColumnInfo(
                            name=name,
                            nativeReferenceName=native_ref,
                            type="hierarchy",
                        )
                        visual_columns.append(hierarchy_info)
            except Exception as e:
                # Fallback to Pydantic models for malformed items
                logger.debug(f"Failed to validate select item: {e}, using fallback")
                name = select_item.get("Name", "")
                native_ref = select_item.get("NativeReferenceName", "")

                if "Measure" in select_item:
                    if not any(m.name == name for m in visual_measures):
                        measure_info = VisualMeasureInfo(
                            name=name,
                            nativeReferenceName=native_ref,
                            entity=name.split(".")[0] if "." in name else "",
                            property=name.split(".")[1] if "." in name else name,
                        )
                        visual_measures.append(measure_info)
                elif "Column" in select_item:
                    if not any(c.name == name for c in visual_columns):
                        column_info = VisualColumnInfo(
                            name=name,
                            nativeReferenceName=native_ref,
                            table=name.split(".")[0] if "." in name else "",
                            column=name.split(".")[1] if "." in name else name,
                        )
                        visual_columns.append(column_info)

    def parse_visualization(self, visual_container: Dict[str, Any]) -> VisualInfo:
        """
        Parse a single visualization container to extract visualization details and columns.
        Uses Pydantic model_validate for type safety and validation.

        Args:
            visual_container: A visual container from the layout

        Returns:
            VisualInfo Pydantic model with structured visualization information
        """
        # Validate visual container structure with Pydantic
        try:
            visual_model = PBIXVisualContainer.model_validate(visual_container)
        except Exception as e:
            logger.warning(f"Failed to validate visual container: {e}")
            # Fallback to basic parsing
            visual_model = PBIXVisualContainer(
                id=visual_container.get("id"),
                x=visual_container.get("x", 0),
                y=visual_container.get("y", 0),
                z=visual_container.get("z", 0),
                width=visual_container.get("width", 0),
                height=visual_container.get("height", 0),
                config=visual_container.get("config", "{}"),
                query=visual_container.get("query", "{}"),
            )

        # Create Pydantic model for visual info
        visual_info = VisualInfo(
            id=visual_model.id,
            position=VisualPosition(
                x=visual_model.x,
                y=visual_model.y,
                z=visual_model.z,
                width=visual_model.width,
                height=visual_model.height,
            ),
            visualType="Unknown",
            columns=[],
            measures=[],
            dataRoles={},
        )

        # Parse config JSON string
        config_str = visual_model.config
        try:
            config = (
                json.loads(config_str) if isinstance(config_str, str) else config_str
            )
            single_visual = config.get("singleVisual", {})
            visual_info.visualType = single_visual.get("visualType", "Unknown")

            # Extract projections (data roles)
            projections = single_visual.get("projections", {})
            for role, refs in projections.items():
                visual_info.dataRoles[role] = []
                for ref in refs:
                    query_ref = ref.get("queryRef", "")
                    visual_info.dataRoles[role].append(
                        DataRole(queryRef=query_ref, active=ref.get("active", False))
                    )

            # Parse prototypeQuery from config for additional column/measure info
            prototype_query = single_visual.get("prototypeQuery", {})
            if prototype_query:
                select_items = prototype_query.get("Select", [])
                self._process_select_items(
                    select_items, visual_info.columns, visual_info.measures
                )
        except (json.JSONDecodeError, AttributeError):
            pass

        # Parse query JSON string to extract columns and measures
        query_str = visual_model.query
        try:
            query = json.loads(query_str) if isinstance(query_str, str) else query_str
            commands = query.get("Commands", [])
            for command in commands:
                semantic_query = command.get("SemanticQueryDataShapeCommand", {})
                query_obj = semantic_query.get("Query", {})
                select_items = query_obj.get("Select", [])

                # Use the same helper to process select items
                self._process_select_items(
                    select_items, visual_info.columns, visual_info.measures
                )
        except (json.JSONDecodeError, AttributeError):
            pass

        # Parse dataTransforms JSON string for additional column details
        data_transforms_str = visual_model.dataTransforms or "{}"
        try:
            data_transforms = (
                json.loads(data_transforms_str)
                if isinstance(data_transforms_str, str)
                else data_transforms_str
            )
            selects = data_transforms.get("selects", [])

            # Enhance column/measure info with display names and types using Pydantic
            for select_data in selects:
                try:
                    select_model = PBIXDataTransformSelect.model_validate(select_data)
                    query_name = select_model.queryName or ""
                    display_name = select_model.displayName or ""
                    roles = select_model.roles
                    data_type = select_model.type
                    format_str = select_model.format

                    # Update existing column/measure info
                    for col in visual_info.columns:
                        if col.name == query_name:
                            col.displayName = display_name
                            col.roles = roles
                            col.dataType = data_type.get("underlyingType")
                            break

                    for measure in visual_info.measures:
                        if measure.name == query_name:
                            measure.displayName = display_name
                            measure.roles = roles
                            measure.dataType = data_type.get("underlyingType")
                            measure.format = format_str
                            break
                except Exception:
                    # Fallback for individual select items
                    query_name = select_data.get("queryName", "")
                    display_name = select_data.get("displayName", "")
                    roles = select_data.get("roles", {})
                    data_type = select_data.get("type", {})

                    for col in visual_info.columns:
                        if col.name == query_name:
                            col.displayName = display_name
                            col.roles = roles
                            col.dataType = data_type.get("underlyingType")
                            break

                    for measure in visual_info.measures:
                        if measure.name == query_name:
                            measure.displayName = display_name
                            measure.roles = roles
                            measure.dataType = data_type.get("underlyingType")
                            measure.format = select_data.get("format")
                            break
        except (json.JSONDecodeError, AttributeError):
            pass

        return visual_info

    def parse_visual_interactions(self, layout: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse visual interaction settings from the layout.
        Visual interactions control how selecting data in one visual filters/highlights others.

        Args:
            layout: The parsed Layout JSON

        Returns:
            Dictionary mapping visual IDs to their interaction settings
        """
        interactions: Dict[str, Any] = {}

        if not layout:
            return interactions

        # Visual interactions are typically in the config of each visual
        sections = layout.get("sections", [])
        for section in sections:
            section_id = section.get("id")
            visual_containers = section.get("visualContainers", [])

            for visual_container in visual_containers:
                viz_id = visual_container.get("id")
                if not viz_id:
                    continue

                # Parse config to find interaction settings
                config_str = visual_container.get("config", "{}")
                try:
                    config = (
                        json.loads(config_str)
                        if isinstance(config_str, str)
                        else config_str
                    )
                    single_visual = config.get("singleVisual", {})

                    # Visual interactions are in various places depending on Power BI version
                    interactions_config = (
                        single_visual.get("visualInteractions")
                        or single_visual.get("interactions")
                        or {}
                    )

                    if interactions_config:
                        interactions[viz_id] = {
                            "sectionId": section_id,
                            "interactions": interactions_config,
                        }
                except (json.JSONDecodeError, AttributeError):
                    pass

        return interactions

    def parse_bookmarks(self, layout: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse bookmarks from the layout using Pydantic validation.
        Bookmarks capture the state of a report page (filters, slicers, visibility, etc.)

        Args:
            layout: The parsed Layout JSON

        Returns:
            List of bookmark definitions
        """
        bookmarks: List[Dict[str, Any]] = []

        if not layout:
            return bookmarks

        # Bookmarks can be in the root of layout or in config
        bookmark_list = layout.get("bookmarks", [])

        for bookmark_data in bookmark_list:
            try:
                # Validate with Pydantic
                bookmark_model = PBIXBookmark.model_validate(bookmark_data)
                bookmarks.append(bookmark_model.model_dump())
            except Exception as e:
                logger.debug(f"Failed to validate bookmark: {e}, using fallback")
                # Fallback
                bookmark = {
                    "name": bookmark_data.get("name", "Unknown"),
                    "displayName": bookmark_data.get("displayName"),
                    "id": bookmark_data.get("id"),
                    "explorationState": bookmark_data.get("explorationState", {}),
                    "options": bookmark_data.get("options", {}),
                    "children": bookmark_data.get("children", []),
                }
                bookmarks.append(bookmark)

        # Also check config for bookmarks
        config = layout.get("config", {})
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except json.JSONDecodeError:
                config = {}

        if config.get("bookmarks"):
            for bookmark_data in config["bookmarks"]:
                try:
                    bookmark_model = PBIXBookmark.model_validate(bookmark_data)
                    bookmarks.append(bookmark_model.model_dump())
                except Exception:
                    bookmark = {
                        "name": bookmark_data.get("name", "Unknown"),
                        "displayName": bookmark_data.get("displayName"),
                        "id": bookmark_data.get("id"),
                        "explorationState": bookmark_data.get("explorationState", {}),
                    }
                    bookmarks.append(bookmark)

        return bookmarks

    def parse_report_parameters(
        self, layout: Dict[str, Any], data_model: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Parse report parameters using Pydantic validation (What-if parameters, query parameters, etc.)

        Args:
            layout: The parsed Layout JSON
            data_model: The parsed DataModel (parameters may be in model)

        Returns:
            List of parameter definitions
        """
        parameters = []

        # Parameters can be in layout config
        if layout:
            config = layout.get("config", {})
            if isinstance(config, str):
                try:
                    config = json.loads(config)
                except json.JSONDecodeError:
                    config = {}

            param_list = config.get("parameters", [])
            for param in param_list:
                try:
                    # Validate with Pydantic
                    param_model = PBIXParameter.model_validate(param)
                    parameters.append(param_model.model_dump(exclude_none=True))
                except Exception:
                    # Fallback
                    parameters.append(
                        {
                            "name": param.get("name", "Unknown"),
                            "value": param.get("value"),
                            "type": param.get("type"),
                        }
                    )

        # Parameters can also be in the data model
        if data_model:
            model = data_model.get("model", {})
            expressions = model.get("expressions", [])

            # What-if parameters are often defined as model expressions
            for expr in expressions:
                if expr.get("kind") == "m":  # M-Query parameter
                    try:
                        param_model = PBIXParameter.model_validate(
                            {
                                "name": expr.get("name", "Unknown"),
                                "expression": expr.get("expression"),
                                "kind": "parameter",
                                "type": "m_query",
                            }
                        )
                        parameters.append(param_model.model_dump(exclude_none=True))
                    except Exception:
                        parameters.append(
                            {
                                "name": expr.get("name", "Unknown"),
                                "expression": expr.get("expression"),
                                "kind": "parameter",
                                "type": "m_query",
                            }
                        )

        return parameters

    def parse_layout(self, layout: Dict[str, Any]) -> PBIXLayoutParsedEnhanced:
        """
        Parse the Layout to extract report information including visualizations and columns.
        Uses Pydantic model_validate for type safety and validation.

        Args:
            layout: The parsed Layout JSON

        Returns:
            PBIXLayoutParsedEnhanced with strongly-typed structured information
        """
        if not layout:
            return PBIXLayoutParsedEnhanced()

        # Validate with Pydantic model
        try:
            layout_model = PBIXLayout.model_validate(layout)
        except Exception as e:
            logger.warning(f"Failed to validate layout structure: {e}")
            # Fallback to basic parsing
            layout_model = PBIXLayout(
                sections=[],
                themes=layout.get("themes", []),
                defaultLayout=layout.get("defaultLayout", {}),
                bookmarks=layout.get("bookmarks", []),
                config=layout.get("config"),
            )
            # Try to parse sections manually
            for section_data in layout.get("sections", []):
                try:
                    section_model = PBIXSection.model_validate(section_data)
                    layout_model.sections.append(section_model)
                except Exception:
                    continue

        parsed_sections: List[SectionInfo] = []
        parsed_visualizations: List[VisualInfo] = []
        bookmarks_list = list(layout_model.bookmarks)
        bookmarks_list.extend(self.parse_bookmarks(layout))
        interactions_dict = self.parse_visual_interactions(layout)

        # Extract sections (pages) with detailed visualization information
        for section_model in layout_model.sections:
            # Parse each visualization in the section
            section_visuals = []
            for visual_container in section_model.visualContainers:
                # Convert Pydantic model to dict for parse_visualization
                visual_container_dict = (
                    visual_container.model_dump()
                    if hasattr(visual_container, "model_dump")
                    else visual_container
                )
                visual_info = self.parse_visualization(visual_container_dict)
                section_visuals.append(visual_info)

                # Also add to top-level visualizations list with section info
                visual_with_section = visual_info.model_copy(deep=True)
                visual_with_section.sectionName = section_model.displayName
                visual_with_section.sectionId = (
                    int(section_model.id) if section_model.id is not None else None
                )
                parsed_visualizations.append(visual_with_section)

            # Create section info model
            section_obj = SectionInfo(
                name=section_model.name,
                displayName=section_model.displayName,
                id=section_model.id,
                width=float(section_model.width),
                height=float(section_model.height),
                visualContainers=section_visuals,
                filters=section_model.filters,
            )
            parsed_sections.append(section_obj)

        return PBIXLayoutParsedEnhanced(
            sections=parsed_sections,
            themes=layout_model.themes,
            default_layout=layout_model.defaultLayout,
            visualizations=parsed_visualizations,
            bookmarks=bookmarks_list,
            interactions=interactions_dict,
        )

    def _find_column_in_data_model(
        self,
        data_model_parsed: Optional[PBIXDataModelParsed],
        table: str,
        column: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Find column details in the data model.

        Args:
            data_model_parsed: Parsed data model (Pydantic model)
            table: Table name to search
            column: Column name to search

        Returns:
            Dictionary with column details if found, None otherwise
        """
        if not data_model_parsed:
            return None

        if data_model_parsed.validated_model:
            dm_model = data_model_parsed.validated_model
            for table_model in dm_model.tables:
                if table_model.name == table:
                    for col_model in table_model.columns:
                        if col_model.name == column:
                            return {
                                "dataType": col_model.dataType,
                                "isHidden": col_model.isHidden,
                                "isNullable": col_model.isNullable,
                                "formatString": col_model.formatString,
                            }
        return None

    def _find_measure_in_data_model(
        self,
        data_model_parsed: Optional[PBIXDataModelParsed],
        entity: str,
        property_name: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Find measure details in the data model.

        Args:
            data_model_parsed: Parsed data model (Pydantic model)
            entity: Table/entity name to search
            property_name: Measure name to search

        Returns:
            Dictionary with measure details if found, None otherwise
        """
        if not data_model_parsed:
            return None

        if data_model_parsed.validated_model:
            dm_model = data_model_parsed.validated_model
            for table_model in dm_model.tables:
                if table_model.name == entity:
                    for measure_model in table_model.measures:
                        if measure_model.name == property_name:
                            return {
                                "expression": measure_model.expression,
                                "formatString": measure_model.formatString,
                                "isHidden": measure_model.isHidden,
                            }
        return None

    def _build_column_lineage_entry(
        self,
        col: VisualColumnInfo,
        viz: VisualInfo,
        data_model_parsed: Optional[PBIXDataModelParsed] = None,
    ) -> ColumnLineageEntry:
        col_name = col.name
        table = col.table
        column = col.column
        display_name = col.displayName or column
        page_name = viz.sectionName or "Unknown"
        page_id = viz.sectionId
        viz_type = viz.visualType
        viz_id = viz.id

        lineage_path = (
            [
                f"Page: {page_name}",
                f"Visualization: {viz_type}",
                f"Column: {col_name}",
                f"Source: {table}.{column}",
            ]
            if table and column
            else [
                f"Page: {page_name}",
                f"Visualization: {viz_type}",
                f"Column: {col_name}",
            ]
        )

        # Add data model details if available
        source_details = None
        if data_model_parsed and table and column:
            source_details = self._find_column_in_data_model(
                data_model_parsed, table, column
            )

        return ColumnLineageEntry(
            visualizationColumn=col_name,
            displayName=display_name,
            sourceTable=table,
            sourceColumn=column,
            dataRole=list(col.roles.keys()) if col.roles else [],
            aggregation=col.aggregation,
            columnType=col.type,
            page=page_name,
            pageId=page_id,
            visualization=viz_type,
            visualizationId=viz_id,
            lineagePath=lineage_path,
            sourceColumnDetails=source_details,
        )

    def _build_measure_lineage_entry(
        self,
        measure: VisualMeasureInfo,
        viz: VisualInfo,
        data_model_parsed: Optional[PBIXDataModelParsed] = None,
    ) -> MeasureLineageEntry:
        measure_name = measure.name
        entity = measure.entity
        property_name = measure.property
        display_name = measure.displayName or property_name
        page_name = viz.sectionName or "Unknown"
        page_id = viz.sectionId
        viz_type = viz.visualType
        viz_id = viz.id

        lineage_path = (
            [
                f"Page: {page_name}",
                f"Visualization: {viz_type}",
                f"Measure: {measure_name}",
                f"Source: {entity}.{property_name}",
            ]
            if entity and property_name
            else [
                f"Page: {page_name}",
                f"Visualization: {viz_type}",
                f"Measure: {measure_name}",
            ]
        )

        # Add data model details if available
        measure_details = None
        if data_model_parsed and entity and property_name:
            measure_details = self._find_measure_in_data_model(
                data_model_parsed, entity, property_name
            )

        return MeasureLineageEntry(
            visualizationMeasure=measure_name,
            displayName=display_name,
            sourceEntity=entity,
            measureName=property_name,
            dataRole=list(measure.roles.keys()) if measure.roles else [],
            format=measure.format,
            page=page_name,
            pageId=page_id,
            visualization=viz_type,
            visualizationId=viz_id,
            lineagePath=lineage_path,
            sourceMeasureDetails=measure_details,
        )

    def _process_viz_pydantic(
        self,
        viz: VisualInfo,
        builder: LineageBuilder,
        data_model_parsed: Optional[PBIXDataModelParsed],
    ) -> None:
        """
        Process visualization from Pydantic model format.

        Args:
            viz: VisualInfo Pydantic model
            builder: LineageBuilder to accumulate results
            data_model_parsed: Parsed data model for enrichment
        """
        viz_lineage = VisualizationLineage(
            visualizationId=viz.id,
            visualizationType=viz.visualType,
            sectionName=viz.sectionName,
            sectionId=viz.sectionId,
            columns=[],
            measures=[],
        )

        for col in viz.columns:
            col_lineage = self._build_column_lineage_entry(col, viz, data_model_parsed)
            viz_lineage.columns.append(col_lineage)
            builder.update_column_mapping(col.table, col.column, viz, col)

        for measure in viz.measures:
            measure_lineage = self._build_measure_lineage_entry(
                measure, viz, data_model_parsed
            )
            viz_lineage.measures.append(measure_lineage)
            builder.update_measure_mapping(
                measure.entity, measure.property, viz, measure
            )

        builder.add_visualization_lineage(viz_lineage)

    def _process_viz_dict(
        self,
        viz: Dict,
        builder: LineageBuilder,
        data_model_parsed: Optional[PBIXDataModelParsed],
    ) -> None:
        """
        Process visualization from dictionary format.

        Args:
            viz: Visualization dictionary
            builder: LineageBuilder to accumulate results
            data_model_parsed: Parsed data model for enrichment
        """
        viz_lineage = VisualizationLineage(
            visualizationId=viz.get("id"),
            visualizationType=viz.get("visualType"),
            sectionName=viz.get("sectionName"),
            sectionId=viz.get("sectionId"),
            columns=[],
            measures=[],
        )

        viz_obj = VisualInfo.model_validate(viz)

        for col_dict in viz.get("columns", []):
            col = VisualColumnInfo.model_validate(col_dict)
            col_lineage = self._build_column_lineage_entry(
                col, viz_obj, data_model_parsed
            )
            viz_lineage.columns.append(col_lineage)
            builder.update_column_mapping(col.table, col.column, viz_obj, col)

        for measure_dict in viz.get("measures", []):
            measure = VisualMeasureInfo.model_validate(measure_dict)
            measure_lineage = self._build_measure_lineage_entry(
                measure, viz_obj, data_model_parsed
            )
            viz_lineage.measures.append(measure_lineage)
            builder.update_measure_mapping(
                measure.entity, measure.property, viz_obj, measure
            )

        builder.add_visualization_lineage(viz_lineage)

    def build_column_lineage(
        self,
        layout_parsed: PBIXLayoutParsedEnhanced,
        data_model_parsed: Optional[PBIXDataModelParsed] = None,
    ) -> PBIXLineageResult:
        """
        Build column-level lineage from parsed layout and data model.

        Args:
            layout_parsed: Parsed layout with visualizations
            data_model_parsed: Parsed data model with tables and columns

        Returns:
            PBIXLineageResult with strongly-typed lineage information
        """
        if not layout_parsed or not layout_parsed.visualizations:
            return PBIXLineageResult()

        # Use LineageBuilder to accumulate results with Pydantic models
        builder = LineageBuilder()

        for viz in layout_parsed.visualizations:
            if isinstance(viz, VisualInfo):
                self._process_viz_pydantic(viz, builder, data_model_parsed)
            else:
                self._process_viz_dict(viz, builder, data_model_parsed)

        # Process page-level lineage
        builder.process_page_lineage_and_mapping(layout_parsed)

        # Finalize and return
        return builder.finalize()

    def extract_metadata(self) -> PBIXExtractedMetadata:
        """
        Main method to extract all metadata from the .pbix file.

        Returns:
            PBIXExtractedMetadata with complete strongly-typed metadata
        """
        extracted = self.extract_zip()

        # Parse data model if available
        data_model_parsed: Optional[PBIXDataModelParsed] = None
        if extracted.data_model:
            data_model_parsed = self.parse_data_model(extracted.data_model)

        # Parse layout if available
        layout_parsed: Optional[PBIXLayoutParsedEnhanced] = None
        parameters: List[Dict[str, Any]] = []
        if extracted.layout:
            layout_parsed = self.parse_layout(extracted.layout)
            parameters = self.parse_report_parameters(
                extracted.layout, extracted.data_model
            )

        # Build column-level lineage
        lineage: Optional[PBIXLineageResult] = None
        if layout_parsed:
            lineage = self.build_column_lineage(layout_parsed, data_model_parsed)

        return PBIXExtractedMetadata(
            file_info=extracted.file_info,
            version=extracted.version,
            metadata=extracted.metadata,
            file_list=extracted.file_list,
            data_model_parsed=data_model_parsed,
            data_model_raw=extracted.data_model,
            layout_parsed=layout_parsed,
            layout_raw=extracted.layout,
            parameters=parameters,
            lineage=lineage,
        )
