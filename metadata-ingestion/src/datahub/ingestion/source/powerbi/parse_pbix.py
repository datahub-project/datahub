import json
import logging
import re
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class PBIXColumn(BaseModel):
    name: str
    dataType: str = "Unknown"
    formatString: Optional[str] = None
    isHidden: bool = False
    isNullable: bool = True
    sourceColumn: Optional[str] = None
    summarizeBy: Optional[str] = None
    expression: Optional[str] = None


class PBIXMeasure(BaseModel):
    name: str
    expression: str = ""
    formatString: Optional[str] = None
    isHidden: bool = False


class PBIXPartition(BaseModel):
    name: str = "Partition"
    mode: str = "Import"
    source: Dict[str, Any] = Field(default_factory=dict)


class PBIXTable(BaseModel):
    name: str
    columns: List[PBIXColumn] = Field(default_factory=list)
    measures: List[PBIXMeasure] = Field(default_factory=list)
    partitions: List[PBIXPartition] = Field(default_factory=list)
    isHidden: bool = False


class PBIXRelationship(BaseModel):
    name: str = ""
    fromTable: str
    fromColumn: str
    toTable: str
    toColumn: str
    crossFilteringBehavior: str = "oneDirection"
    isActive: bool = True


class PBIXDataModel(BaseModel):
    tables: List[PBIXTable] = Field(default_factory=list)
    relationships: List[PBIXRelationship] = Field(default_factory=list)
    cultures: str = "en-US"
    version: str = "Unknown"


class PBIXDataModelRoot(BaseModel):
    model: Dict[str, Any] = Field(default_factory=dict)


class PBIXAggregationExpression(BaseModel):
    Column: Optional[Dict[str, Any]] = None
    Property: Optional[Dict[str, Any]] = None


class PBIXAggregation(BaseModel):
    Function: int = 0
    Expression: PBIXAggregationExpression = Field(
        default_factory=PBIXAggregationExpression
    )


class PBIXSelectItem(BaseModel):
    Aggregation: Optional[PBIXAggregation] = None
    Measure: Optional[Dict[str, Any]] = None
    Column: Optional[Dict[str, Any]] = None
    HierarchyLevel: Optional[Dict[str, Any]] = None
    Name: Optional[str] = None
    NativeReferenceName: Optional[str] = None


class PBIXBookmark(BaseModel):
    name: str
    displayName: Optional[str] = None
    id: Optional[str] = None
    explorationState: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] = Field(default_factory=dict)
    children: List[str] = Field(default_factory=list)


class PBIXParameter(BaseModel):
    name: str
    value: Optional[Union[str, int, float, bool]] = None
    type: Optional[str] = None
    expression: Optional[str] = None
    kind: Optional[str] = None


class PBIXDataTransformSelect(BaseModel):
    queryName: Optional[str] = None
    displayName: Optional[str] = None
    roles: Dict[str, bool] = Field(default_factory=dict)
    type: Dict[str, Any] = Field(default_factory=dict)
    format: Optional[str] = None


# Visual Information Models (to replace dictionaries)
class VisualPosition(BaseModel):
    x: float = 0.0
    y: float = 0.0
    z: int = 0
    width: float = 0.0
    height: float = 0.0


class VisualColumnInfo(BaseModel):
    name: str
    nativeReferenceName: str = ""
    table: str = ""
    column: str = ""
    displayName: str = ""
    roles: Dict[str, bool] = Field(default_factory=dict)
    dataType: Optional[str] = None
    aggregation: Optional[str] = None
    type: str = "column"


class VisualMeasureInfo(BaseModel):
    name: str
    nativeReferenceName: str = ""
    entity: str = ""
    property: str = ""
    displayName: str = ""
    roles: Dict[str, bool] = Field(default_factory=dict)
    dataType: Optional[str] = None
    format: Optional[str] = None


class DataRole(BaseModel):
    queryRef: str
    active: bool = False


class VisualInfo(BaseModel):
    id: Optional[str] = None
    position: VisualPosition = Field(default_factory=VisualPosition)
    visualType: str = "Unknown"
    columns: List[VisualColumnInfo] = Field(default_factory=list)
    measures: List[VisualMeasureInfo] = Field(default_factory=list)
    dataRoles: Dict[str, List[DataRole]] = Field(default_factory=dict)
    sectionName: Optional[str] = None
    sectionId: Optional[str] = None


class SectionInfo(BaseModel):
    name: str
    displayName: str
    id: Optional[str] = None
    width: float = 0.0
    height: float = 0.0
    visualContainers: List[VisualInfo] = Field(default_factory=list)
    filters: List[Any] = Field(default_factory=list)


class LayoutParsed(BaseModel):
    sections: List[SectionInfo] = Field(default_factory=list)
    themes: List[Any] = Field(default_factory=list)
    defaultLayout: Dict[str, Any] = Field(default_factory=dict)
    visualizations: List[VisualInfo] = Field(default_factory=list)
    bookmarks: List[Dict[str, Any]] = Field(default_factory=list)
    interactions: Dict[str, Any] = Field(default_factory=dict)


# Lineage Models (to replace dictionaries)
class ColumnLineageEntry(BaseModel):
    visualizationColumn: str
    displayName: str = ""
    sourceTable: str = ""
    sourceColumn: str = ""
    dataRole: List[str] = Field(default_factory=list)
    aggregation: Optional[str] = None
    columnType: str = "column"
    page: str = ""
    pageId: Optional[str] = None
    visualization: str = ""
    visualizationId: Optional[str] = None
    lineagePath: List[str] = Field(default_factory=list)
    sourceColumnDetails: Optional[Dict[str, Any]] = None


class MeasureLineageEntry(BaseModel):
    visualizationMeasure: str
    displayName: str = ""
    sourceEntity: str = ""
    measureName: str = ""
    dataRole: List[str] = Field(default_factory=list)
    format: Optional[str] = None
    page: str = ""
    pageId: Optional[str] = None
    visualization: str = ""
    visualizationId: Optional[str] = None
    lineagePath: List[str] = Field(default_factory=list)
    sourceMeasureDetails: Optional[Dict[str, Any]] = None


class VisualizationLineage(BaseModel):
    visualizationId: Optional[str] = None
    visualizationType: str = "Unknown"
    sectionName: Optional[str] = None
    sectionId: Optional[str] = None
    columns: List[ColumnLineageEntry] = Field(default_factory=list)
    measures: List[MeasureLineageEntry] = Field(default_factory=list)


class PBIXVisualConfig(BaseModel):
    singleVisual: Dict[str, Any] = Field(default_factory=dict)


class PBIXVisualContainer(BaseModel):
    id: Optional[str] = None
    x: int = 0
    y: int = 0
    z: int = 0
    width: int = 0
    height: int = 0
    config: Union[str, Dict[str, Any]] = "{}"
    query: Union[str, Dict[str, Any]] = "{}"
    dataTransforms: Optional[Union[str, Dict[str, Any]]] = None


class PBIXSection(BaseModel):
    name: str = ""
    displayName: str = ""
    id: Optional[str] = None
    width: int = 0
    height: int = 0
    visualContainers: List[Dict[str, Any]] = Field(default_factory=list)
    filters: List[Any] = Field(default_factory=list)


class PBIXLayout(BaseModel):
    sections: List[PBIXSection] = Field(default_factory=list)
    themes: List[Any] = Field(default_factory=list)
    defaultLayout: Dict[str, Any] = Field(default_factory=dict)
    bookmarks: List[Dict[str, Any]] = Field(default_factory=list)
    config: Optional[Union[str, Dict[str, Any]]] = None


class ColumnReference(BaseModel):
    name: str = Field(description="Full column reference name")
    table: str = Field(default="", description="Source table name")
    column: str = Field(default="", description="Column name")
    display_name: str = Field(
        default="", description="Display name shown in the visual"
    )
    roles: Dict[str, bool] = Field(
        default_factory=dict, description="Data roles this column fills"
    )


class MeasureReference(BaseModel):
    name: str = Field(description="Full measure reference name")
    entity: str = Field(default="", description="Table/entity containing the measure")
    property: str = Field(default="", description="Measure property name")
    display_name: str = Field(
        default="", description="Display name shown in the visual"
    )
    roles: Dict[str, bool] = Field(
        default_factory=dict, description="Data roles this measure fills"
    )


class VisualizationMetadata(BaseModel):
    id: str = Field(description="Unique identifier for the visualization")
    visual_type: str = Field(
        description="Type of visual (e.g., 'barChart', 'table', 'pieChart')"
    )
    position_x: float = Field(default=0.0, description="X coordinate of visual")
    position_y: float = Field(default=0.0, description="Y coordinate of visual")
    width: float = Field(default=0.0, description="Width of visual")
    height: float = Field(default=0.0, description="Height of visual")
    z_index: int = Field(default=0, description="Z-index (layering) of visual")
    columns: List[ColumnReference] = Field(
        default_factory=list, description="Column references used"
    )
    measures: List[MeasureReference] = Field(
        default_factory=list, description="Measure references used"
    )
    filters: List[str] = Field(default_factory=list, description="Visual-level filters")
    title: Optional[str] = Field(default=None, description="Visual title if set")

    # Raw config preserved for complex scenarios
    config_raw: Optional[Dict] = Field(
        default=None, description="Raw visual configuration"
    )


class BookmarkMetadata(BaseModel):
    name: str = Field(description="Internal name of the bookmark")
    display_name: Optional[str] = Field(
        default=None, description="User-facing display name"
    )
    id: str = Field(description="Unique identifier for the bookmark")
    exploration_state: Dict = Field(
        default_factory=dict, description="Captured state (filters, slicers, etc.)"
    )
    options: Dict = Field(
        default_factory=dict, description="Bookmark options and settings"
    )
    children: List[str] = Field(default_factory=list, description="Child bookmark IDs")


class ParameterMetadata(BaseModel):
    name: str = Field(description="Parameter name")
    parameter_type: Literal["what_if", "query", "m_query", "field"] = Field(
        description="Type of parameter"
    )
    value: Optional[Union[str, int, float, bool]] = Field(
        default=None, description="Current parameter value"
    )
    expression: Optional[str] = Field(
        default=None, description="M-Query or DAX expression defining the parameter"
    )


class VisualInteraction(BaseModel):
    visual_id: str = Field(description="ID of the visual")
    section_id: str = Field(description="Page/section ID containing the visual")
    interactions: Dict = Field(
        default_factory=dict, description="Interaction configuration with other visuals"
    )


class PageMetadata(BaseModel):
    name: str = Field(description="Internal page name")
    display_name: str = Field(description="User-facing page display name")
    id: str = Field(description="Unique page identifier")
    width: float = Field(default=0.0, description="Page width")
    height: float = Field(default=0.0, description="Page height")
    visualizations: List[VisualizationMetadata] = Field(
        default_factory=list, description="Visualizations on this page"
    )
    filters: List[str] = Field(default_factory=list, description="Page-level filters")


class PBIXMetadata(BaseModel):
    file_info: Dict[str, Union[str, int]] = Field(
        description="File information (name, size, etc.)"
    )
    version: Optional[str] = Field(default=None, description="Power BI file version")
    pages: List[PageMetadata] = Field(default_factory=list, description="Report pages")
    bookmarks: List[BookmarkMetadata] = Field(
        default_factory=list, description="Report bookmarks"
    )
    parameters: List[ParameterMetadata] = Field(
        default_factory=list, description="Report parameters"
    )
    interactions: List[VisualInteraction] = Field(
        default_factory=list, description="Visual interaction settings"
    )

    # Keep raw data for backward compatibility and complex scenarios
    data_model_raw: Optional[Dict] = Field(
        default=None, description="Raw data model JSON"
    )
    layout_raw: Optional[Dict] = Field(default=None, description="Raw layout JSON")


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

    def extract_zip(self) -> Dict[str, Any]:
        """
        Extract and parse the .pbix ZIP archive.

        Returns:
            Dictionary containing parsed metadata
        """
        result: Dict[str, Any] = {
            "file_info": {
                "filename": self.pbix_path.name,
                "size_bytes": self.pbix_path.stat().st_size,
            },
            "version": None,
            "metadata": None,
            "data_model": None,
            "layout": None,
            "file_list": [],
        }

        try:
            with zipfile.ZipFile(self.pbix_path, "r") as zip_ref:
                # List all files in the archive
                file_list = zip_ref.namelist()
                result["file_list"] = sorted(file_list)

                # Extract and parse key files
                for filename in file_list:
                    try:
                        if filename == "Version":
                            content = zip_ref.read(filename).decode("utf-8")
                            result["version"] = content.strip()

                        elif filename == "Metadata":
                            content = zip_ref.read(filename).decode("utf-8")
                            # Metadata might not be JSON, try to parse it
                            try:
                                result["metadata"] = json.loads(content)
                            except json.JSONDecodeError:
                                # Metadata might be in a different format, store as raw text
                                result["metadata"] = content

                        elif filename in ("DataModelSchema", "DataModel"):
                            raw_content = zip_ref.read(filename)
                            # Try UTF-16LE first (common in .pbix files), then UTF-8
                            try:
                                content = raw_content.decode("utf-16-le")
                            except UnicodeDecodeError:
                                try:
                                    content = raw_content.decode("utf-8")
                                except UnicodeDecodeError:
                                    # If it's compressed or binary, skip for now
                                    logger.warning(
                                        f"Could not decode {filename} (might be compressed)"
                                    )
                                    continue

                            # Check if it's compressed (XPress9)
                            if content.startswith(
                                "This backup was created using XPress9 compression."
                            ):
                                logger.warning(
                                    f"{filename} is compressed with XPress9 (decompression not yet supported)"
                                )
                                continue

                            result["data_model"] = json.loads(content)

                        elif filename in ("Layout", "Report/Layout"):
                            raw_content = zip_ref.read(filename)
                            # Try UTF-16LE first (common in .pbix files), then UTF-8
                            try:
                                content = raw_content.decode("utf-16-le")
                            except UnicodeDecodeError:
                                content = raw_content.decode("utf-8")
                            result["layout"] = json.loads(content)

                    except (UnicodeDecodeError, json.JSONDecodeError) as e:
                        # Some files might be binary or have encoding issues
                        logger.warning(f"Could not parse {filename}: {e}")
                        continue

        except zipfile.BadZipFile as e:
            raise ValueError(f"Invalid ZIP file: {self.pbix_path}") from e

        return result

    def parse_data_model(self, data_model: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the DataModelSchema to extract tables, columns, relationships, etc.
        Uses Pydantic models for validation and type safety.

        Args:
            data_model: The parsed DataModelSchema JSON

        Returns:
            Structured information about the data model
        """
        if not data_model:
            return {}

        try:
            # Extract model structure
            model = data_model.get("model", {})
            tables_data = model.get("tables", [])
            relationships_data = model.get("relationships", [])

            # Parse tables using Pydantic model_validate
            tables = []
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

                    # Create table model with validation
                    table = PBIXTable.model_validate(
                        {
                            "name": table_data.get("name", "Unknown"),
                            "columns": columns,
                            "measures": measures,
                            "partitions": partitions,
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

            # Create complete data model with validation
            data_model_obj = PBIXDataModel.model_validate(
                {
                    "tables": tables,
                    "relationships": relationships,
                    "cultures": data_model.get("culture", "en-US"),
                    "version": data_model.get("version", "Unknown"),
                }
            )

            # Convert back to dict for backward compatibility
            parsed = {
                "tables": [
                    {
                        "name": table.name,
                        "columns": [col.model_dump() for col in table.columns],
                        "measures": [
                            measure.model_dump() for measure in table.measures
                        ],
                        "partitions": [
                            partition.model_dump() for partition in table.partitions
                        ],
                        "isHidden": table.isHidden,
                    }
                    for table in data_model_obj.tables
                ],
                "relationships": [
                    rel.model_dump() for rel in data_model_obj.relationships
                ],
                "cultures": data_model_obj.cultures,
                "version": data_model_obj.version,
                "validated_model": data_model_obj,  # Keep for type-safe access
            }
        except Exception as e:
            logger.error(f"Failed to parse data model: {e}")
            # Fallback to basic structure
            parsed = {
                "tables": [],
                "relationships": [],
                "cultures": data_model.get("culture", "en-US"),
                "version": data_model.get("version", "Unknown"),
            }

        return parsed

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

    def parse_layout(self, layout: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse the Layout to extract report information including visualizations and columns.
        Uses Pydantic model_validate for type safety and validation.

        Args:
            layout: The parsed Layout JSON

        Returns:
            Structured information about the report layout
        """
        if not layout:
            return {}

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

        parsed: Dict[str, Any] = {
            "sections": [],
            "themes": layout_model.themes,
            "defaultLayout": layout_model.defaultLayout,
            "visualizations": [],
            "bookmarks": list(layout_model.bookmarks),
            "interactions": {},
        }

        # Extract bookmarks (combine from model and method)
        parsed["bookmarks"].extend(self.parse_bookmarks(layout))

        # Extract visual interactions
        parsed["interactions"] = self.parse_visual_interactions(layout)

        # Extract sections (pages) with detailed visualization information
        for section_model in layout_model.sections:
            # Parse each visualization in the section
            section_visuals = []
            for visual_container in section_model.visualContainers:
                # Convert dict to Pydantic model
                visual_info = self.parse_visualization(visual_container)
                section_visuals.append(visual_info)

                # Also add to top-level visualizations list with section info
                visual_with_section = visual_info.model_copy(deep=True)
                visual_with_section.sectionName = section_model.displayName
                visual_with_section.sectionId = section_model.id
                parsed["visualizations"].append(visual_with_section)

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
            parsed["sections"].append(section_obj)

        return parsed

    def _find_column_in_data_model(
        self, data_model_parsed: Dict[str, Any], table: str, column: str
    ) -> Optional[Dict[str, Any]]:
        # Try to use validated data model if available
        if "validated_model" in data_model_parsed:
            dm_model: PBIXDataModel = data_model_parsed["validated_model"]
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

        # Fallback to dict parsing
        for table_info in data_model_parsed.get("tables", []):
            if table_info.get("name") == table:
                for col_info in table_info.get("columns", []):
                    if col_info.get("name") == column:
                        return {
                            "dataType": col_info.get("dataType"),
                            "isHidden": col_info.get("isHidden"),
                            "isNullable": col_info.get("isNullable"),
                            "formatString": col_info.get("formatString"),
                        }
        return None

    def _find_measure_in_data_model(
        self, data_model_parsed: Dict[str, Any], entity: str, property_name: str
    ) -> Optional[Dict[str, Any]]:
        # Try to use validated data model if available
        if "validated_model" in data_model_parsed:
            dm_model: PBIXDataModel = data_model_parsed["validated_model"]
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

        # Fallback to dict parsing
        for table_info in data_model_parsed.get("tables", []):
            if table_info.get("name") == entity:
                for measure_info in table_info.get("measures", []):
                    if measure_info.get("name") == property_name:
                        return {
                            "expression": measure_info.get("expression"),
                            "formatString": measure_info.get("formatString"),
                            "isHidden": measure_info.get("isHidden"),
                        }
        return None

    def _build_column_lineage_entry(
        self,
        col: VisualColumnInfo,
        viz: VisualInfo,
        data_model_parsed: Optional[Dict] = None,
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
        data_model_parsed: Optional[Dict] = None,
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

    def _update_column_mapping(
        self,
        lineage: Dict,
        table: str,
        column: str,
        viz: VisualInfo,
        col: VisualColumnInfo,
    ) -> None:
        if not (table and column):
            return

        key = f"{table}.{column}"
        if key not in lineage["column_mapping"]:
            lineage["column_mapping"][key] = {
                "table": table,
                "column": column,
                "usedInVisualizations": [],
            }

        lineage["column_mapping"][key]["usedInVisualizations"].append(
            {
                "visualizationId": viz.id,
                "visualizationType": viz.visualType,
                "page": viz.sectionName,
                "pageId": viz.sectionId,
                "dataRole": list(col.roles.keys()) if col.roles else [],
            }
        )

        # Update table usage
        if table not in lineage["table_usage"]:
            lineage["table_usage"][table] = {
                "columns": set(),
                "visualizations": [],
            }
        lineage["table_usage"][table]["columns"].add(column)
        lineage["table_usage"][table]["visualizations"].append(
            {
                "id": viz.id,
                "type": viz.visualType,
                "page": viz.sectionName,
                "pageId": viz.sectionId,
            }
        )

    def _update_measure_mapping(
        self,
        lineage: Dict,
        entity: str,
        property_name: str,
        viz: VisualInfo,
        measure: VisualMeasureInfo,
    ) -> None:
        if not (entity and property_name):
            return

        key = f"{entity}.{property_name}"
        if key not in lineage["measure_mapping"]:
            lineage["measure_mapping"][key] = {
                "entity": entity,
                "measure": property_name,
                "usedInVisualizations": [],
            }

        lineage["measure_mapping"][key]["usedInVisualizations"].append(
            {
                "visualizationId": viz.id,
                "visualizationType": viz.visualType,
                "page": viz.sectionName,
                "pageId": viz.sectionId,
                "dataRole": list(measure.roles.keys()) if measure.roles else [],
            }
        )

    def _process_page_lineage_and_mapping(
        self, lineage: Dict, layout_parsed: Dict[str, Any]
    ) -> None:
        for section in layout_parsed.get("sections", []):
            page_name = section.get("displayName", "Unknown")
            page_id = section.get("id")

            if page_name not in lineage["page_lineage"]:
                lineage["page_lineage"][page_name] = {
                    "pageId": page_id,
                    "pageName": page_name,
                    "columns": {},
                    "measures": {},
                    "tables": set(),
                    "visualizations": [],
                }

            for viz in section.get("visualContainers", []):
                viz_type = viz.get("visualType", "Unknown")
                viz_id = viz.get("id")

                lineage["page_lineage"][page_name]["visualizations"].append(
                    {"id": viz_id, "type": viz_type}
                )

                for col in viz.get("columns", []):
                    table = col.get("table", "")
                    column = col.get("column", "")
                    if table and column:
                        col_key = f"{table}.{column}"
                        if col_key not in lineage["page_lineage"][page_name]["columns"]:
                            lineage["page_lineage"][page_name]["columns"][col_key] = {
                                "table": table,
                                "column": column,
                                "visualizations": [],
                            }
                        lineage["page_lineage"][page_name]["columns"][col_key][
                            "visualizations"
                        ].append(
                            {
                                "id": viz_id,
                                "type": viz_type,
                                "dataRole": list(col.get("roles", {}).keys())
                                if col.get("roles")
                                else [],
                            }
                        )
                        lineage["page_lineage"][page_name]["tables"].add(table)

                for measure in viz.get("measures", []):
                    entity = measure.get("entity", "")
                    property_name = measure.get("property", "")
                    if entity and property_name:
                        measure_key = f"{entity}.{property_name}"
                        if (
                            measure_key
                            not in lineage["page_lineage"][page_name]["measures"]
                        ):
                            lineage["page_lineage"][page_name]["measures"][
                                measure_key
                            ] = {
                                "entity": entity,
                                "measure": property_name,
                                "visualizations": [],
                            }
                        lineage["page_lineage"][page_name]["measures"][measure_key][
                            "visualizations"
                        ].append(
                            {
                                "id": viz_id,
                                "type": viz_type,
                                "dataRole": list(measure.get("roles", {}).keys())
                                if measure.get("roles")
                                else [],
                            }
                        )

            for col_key, col_info in lineage["page_lineage"][page_name][
                "columns"
            ].items():
                if col_key not in lineage["page_mapping"]:
                    lineage["page_mapping"][col_key] = {
                        "table": col_info["table"],
                        "column": col_info["column"],
                        "usedInPages": [],
                    }
                lineage["page_mapping"][col_key]["usedInPages"].append(
                    {
                        "page": page_name,
                        "pageId": page_id,
                        "visualizationCount": len(col_info["visualizations"]),
                    }
                )

            for measure_key, measure_info in lineage["page_lineage"][page_name][
                "measures"
            ].items():
                if measure_key not in lineage["page_mapping"]:
                    lineage["page_mapping"][measure_key] = {
                        "entity": measure_info["entity"],
                        "measure": measure_info["measure"],
                        "usedInPages": [],
                    }
                lineage["page_mapping"][measure_key]["usedInPages"].append(
                    {
                        "page": page_name,
                        "pageId": page_id,
                        "visualizationCount": len(measure_info["visualizations"]),
                    }
                )

    def _finalize_lineage_sets(self, lineage: Dict) -> None:
        for table in lineage["table_usage"]:
            lineage["table_usage"][table]["columns"] = sorted(
                list(lineage["table_usage"][table]["columns"])
            )
            seen = set()
            unique_viz = []
            for viz in lineage["table_usage"][table]["visualizations"]:
                viz_key = (viz["id"], viz["type"], viz.get("page"))
                if viz_key not in seen:
                    seen.add(viz_key)
                    unique_viz.append(viz)
            lineage["table_usage"][table]["visualizations"] = unique_viz

        for page_name in lineage["page_lineage"]:
            lineage["page_lineage"][page_name]["tables"] = sorted(
                list(lineage["page_lineage"][page_name]["tables"])
            )

    def _process_viz_pydantic(
        self, viz: VisualInfo, lineage: Dict, data_model_parsed: Optional[Dict]
    ) -> None:
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
            self._update_column_mapping(lineage, col.table, col.column, viz, col)

        for measure in viz.measures:
            measure_lineage = self._build_measure_lineage_entry(
                measure, viz, data_model_parsed
            )
            viz_lineage.measures.append(measure_lineage)
            self._update_measure_mapping(
                lineage, measure.entity, measure.property, viz, measure
            )

        lineage["visualization_lineage"].append(viz_lineage.model_dump())

    def _process_viz_dict(
        self, viz: Dict, lineage: Dict, data_model_parsed: Optional[Dict]
    ) -> None:
        viz_lineage: Dict[str, Any] = {
            "visualizationId": viz.get("id"),
            "visualizationType": viz.get("visualType"),
            "sectionName": viz.get("sectionName"),
            "sectionId": viz.get("sectionId"),
            "columns": [],
            "measures": [],
        }

        for col_dict in viz.get("columns", []):
            col = VisualColumnInfo.model_validate(col_dict)
            viz_obj = VisualInfo.model_validate(viz)
            col_lineage = self._build_column_lineage_entry(
                col, viz_obj, data_model_parsed
            )
            viz_lineage["columns"].append(col_lineage.model_dump())
            self._update_column_mapping(lineage, col.table, col.column, viz_obj, col)

        for measure_dict in viz.get("measures", []):
            measure = VisualMeasureInfo.model_validate(measure_dict)
            viz_obj = VisualInfo.model_validate(viz)
            measure_lineage = self._build_measure_lineage_entry(
                measure, viz_obj, data_model_parsed
            )
            viz_lineage["measures"].append(measure_lineage.model_dump())
            self._update_measure_mapping(
                lineage, measure.entity, measure.property, viz_obj, measure
            )

        lineage["visualization_lineage"].append(viz_lineage)

    def build_column_lineage(
        self,
        layout_parsed: Dict[str, Any],
        data_model_parsed: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        lineage: Dict[str, Any] = {
            "visualization_lineage": [],
            "column_mapping": {},
            "measure_mapping": {},
            "table_usage": {},
            "page_lineage": {},
            "page_mapping": {},
        }

        if not layout_parsed or "visualizations" not in layout_parsed:
            return lineage

        visualizations = layout_parsed.get("visualizations", [])
        for viz in visualizations:
            if isinstance(viz, VisualInfo):
                self._process_viz_pydantic(viz, lineage, data_model_parsed)
            else:
                self._process_viz_dict(viz, lineage, data_model_parsed)

        self._process_page_lineage_and_mapping(lineage, layout_parsed)
        self._finalize_lineage_sets(lineage)

        return lineage

    def _old_build_page_lineage_loop(self, lineage: Dict, layout_parsed: Dict) -> None:
        for section in layout_parsed.get("sections", []):
            page_name = section.get("displayName", "Unknown")
            page_id = section.get("id")

            if page_name not in lineage["page_lineage"]:
                lineage["page_lineage"][page_name] = {
                    "pageId": page_id,
                    "pageName": page_name,
                    "columns": {},
                    "measures": {},
                    "tables": set(),
                    "visualizations": [],
                }

            # Collect all columns and measures used on this page
            for viz in section.get("visualContainers", []):
                viz_type = viz.get("visualType", "Unknown")
                viz_id = viz.get("id")

                lineage["page_lineage"][page_name]["visualizations"].append(
                    {"id": viz_id, "type": viz_type}
                )

                # Add columns from this visualization
                for col in viz.get("columns", []):
                    table = col.get("table", "")
                    column = col.get("column", "")
                    if table and column:
                        col_key = f"{table}.{column}"
                        if col_key not in lineage["page_lineage"][page_name]["columns"]:
                            lineage["page_lineage"][page_name]["columns"][col_key] = {
                                "table": table,
                                "column": column,
                                "visualizations": [],
                            }
                        lineage["page_lineage"][page_name]["columns"][col_key][
                            "visualizations"
                        ].append(
                            {
                                "id": viz_id,
                                "type": viz_type,
                                "dataRole": list(col.get("roles", {}).keys())
                                if col.get("roles")
                                else [],
                            }
                        )
                        lineage["page_lineage"][page_name]["tables"].add(table)

                # Add measures from this visualization
                for measure in viz.get("measures", []):
                    entity = measure.get("entity", "")
                    property_name = measure.get("property", "")
                    if entity and property_name:
                        measure_key = f"{entity}.{property_name}"
                        if (
                            measure_key
                            not in lineage["page_lineage"][page_name]["measures"]
                        ):
                            lineage["page_lineage"][page_name]["measures"][
                                measure_key
                            ] = {
                                "entity": entity,
                                "measure": property_name,
                                "visualizations": [],
                            }
                        lineage["page_lineage"][page_name]["measures"][measure_key][
                            "visualizations"
                        ].append(
                            {
                                "id": viz_id,
                                "type": viz_type,
                                "dataRole": list(measure.get("roles", {}).keys())
                                if measure.get("roles")
                                else [],
                            }
                        )

            # Build page mapping (reverse: which pages use each column/measure)
            for col_key, col_info in lineage["page_lineage"][page_name][
                "columns"
            ].items():
                if col_key not in lineage["page_mapping"]:
                    lineage["page_mapping"][col_key] = {
                        "table": col_info["table"],
                        "column": col_info["column"],
                        "usedInPages": [],
                    }
                lineage["page_mapping"][col_key]["usedInPages"].append(
                    {
                        "page": page_name,
                        "pageId": page_id,
                        "visualizationCount": len(col_info["visualizations"]),
                    }
                )

            for measure_key, measure_info in lineage["page_lineage"][page_name][
                "measures"
            ].items():
                if measure_key not in lineage["page_mapping"]:
                    lineage["page_mapping"][measure_key] = {
                        "entity": measure_info["entity"],
                        "measure": measure_info["measure"],
                        "usedInPages": [],
                    }
                lineage["page_mapping"][measure_key]["usedInPages"].append(
                    {
                        "page": page_name,
                        "pageId": page_id,
                        "visualizationCount": len(measure_info["visualizations"]),
                    }
                )

        # Convert sets to lists for JSON serialization
        for table in lineage["table_usage"]:
            lineage["table_usage"][table]["columns"] = sorted(
                list(lineage["table_usage"][table]["columns"])
            )
            # Remove duplicates from visualizations list
            seen = set()
            unique_viz = []
            for viz in lineage["table_usage"][table]["visualizations"]:
                viz_key = (viz["id"], viz["type"], viz.get("page"))
                if viz_key not in seen:
                    seen.add(viz_key)
                    unique_viz.append(viz)
            lineage["table_usage"][table]["visualizations"] = unique_viz

        # Convert sets in page_lineage to lists
        for page_name in lineage["page_lineage"]:
            lineage["page_lineage"][page_name]["tables"] = sorted(
                list(lineage["page_lineage"][page_name]["tables"])
            )

        return lineage  # type: ignore[return-value]

    def extract_metadata(self) -> Dict[str, Any]:
        """
        Main method to extract all metadata from the .pbix file.

        Returns:
            Complete metadata dictionary
        """
        extracted = self.extract_zip()

        result = {
            "file_info": extracted["file_info"],
            "version": extracted["version"],
            "metadata": extracted["metadata"],
            "file_list": extracted["file_list"],
        }

        # Parse data model if available
        if extracted["data_model"]:
            result["data_model_parsed"] = self.parse_data_model(extracted["data_model"])
            result["data_model_raw"] = extracted["data_model"]  # Keep raw for reference

        # Parse layout if available
        if extracted["layout"]:
            result["layout_parsed"] = self.parse_layout(extracted["layout"])
            result["layout_raw"] = extracted["layout"]  # Keep raw for reference

            # Extract report parameters
            result["parameters"] = self.parse_report_parameters(
                extracted["layout"], extracted.get("data_model")
            )

        # Build column-level lineage
        if extracted["layout"]:
            data_model = (
                result.get("data_model_parsed") if extracted.get("data_model") else None
            )
            result["lineage"] = self.build_column_lineage(
                result.get("layout_parsed", {}), data_model
            )

        return result
