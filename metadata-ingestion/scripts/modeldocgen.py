import glob
import json
import logging
import os
import re
import shutil
import unittest.mock
from dataclasses import Field, dataclass, field
from datetime import datetime, timezone
from enum import auto
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import avro.schema
import click
from utils import should_write_json_file

from datahub.configuration.common import ConfigEnum, PermissiveConfigModel
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsClass,
    BrowsePathsV2Class,
    DatasetPropertiesClass,
    ForeignKeyConstraintClass,
    GlobalTagsClass,
    OtherSchemaClass,
    SchemaFieldClass as SchemaField,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyDefinitionClass,
    StructuredPropertySettingsClass,
    StructuredPropertyValueAssignmentClass,
    SubTypesClass,
    TagAssociationClass,
)
from datahub.metadata.urns import StructuredPropertyUrn, Urn

logger = logging.getLogger(__name__)


# TODO: Support generating docs for each event type in entity registry.


def capitalize_first(something: str) -> str:
    return something[0:1].upper() + something[1:]


class EntityCategory(ConfigEnum):
    CORE = auto()
    INTERNAL = auto()


@dataclass
class EntityDefinition:
    name: str
    keyAspect: str
    aspects: List[str] = field(default_factory=list)
    aspect_map: Optional[Dict[str, Any]] = None
    relationship_map: Optional[Dict[str, str]] = None
    doc: Optional[str] = None
    doc_file_contents: Optional[str] = None
    # entities are by default in the CORE category unless specified otherwise
    category: EntityCategory = EntityCategory.CORE
    priority: Optional[int] = None
    # schema: Optional[avro.schema.Schema] = None
    # logical_schema: Optional[avro.schema.Schema] = None

    # @validator("name")
    # def lower_everything(cls, v: str) -> str:
    #    return v.lower()

    @property
    def display_name(self):
        return capitalize_first(self.name)


@dataclass
class AspectDefinition:
    name: str
    EntityUrns: Optional[List[str]] = None
    schema: Optional[avro.schema.Schema] = None
    type: Optional[str] = None


@dataclass
class EventDefinition:
    name: str


# New dataclasses for lineage representation
@dataclass
class LineageRelationship:
    name: str
    entityTypes: List[str]
    isLineage: bool = True


@dataclass
class LineageField:
    name: str
    path: str
    isLineage: bool = True
    relationship: Optional[LineageRelationship] = None


@dataclass
class LineageAspect:
    aspect: str
    fields: List[LineageField]


@dataclass
class LineageEntity:
    aspects: Dict[str, LineageAspect]


@dataclass
class LineageData:
    entities: Dict[str, LineageEntity]


entity_registry: Dict[str, EntityDefinition] = {}


def get_aspects_from_snapshot(
    snapshot_schema: avro.schema.RecordSchema,
) -> Dict[str, AspectDefinition]:
    union_schema: avro.schema.UnionSchema = snapshot_schema.fields[1].type.items
    aspect_map = {}
    for aspect_schema in union_schema.schemas:
        if "Aspect" in aspect_schema.props:
            aspectDef = AspectDefinition(
                schema=aspect_schema,
                name=aspect_schema.props["Aspect"].get("name"),
            )
            aspect_map[aspectDef.name] = aspectDef

    return aspect_map


aspect_registry: Dict[str, AspectDefinition] = {}

# A holder for generated docs
generated_documentation: Dict[str, str] = {}

# A holder for generated DataHub-optimized docs (without Docusaurus-specific features)
generated_documentation_datahub: Dict[str, str] = {}


# Patch add_name method to NOT complain about duplicate names
def add_name(self, name_attr, space_attr, new_schema):
    to_add = avro.schema.Name(name_attr, space_attr, self.default_namespace)

    if self.names:
        self.names[to_add.fullname] = new_schema
    return to_add


def load_schema_file(schema_file: str) -> None:
    logger.debug(f"Loading schema file: {schema_file}")

    with open(schema_file) as f:
        raw_schema_text = f.read()

    avro_schema = avro.schema.parse(raw_schema_text)

    if (
        isinstance(avro_schema, avro.schema.RecordSchema)
        and "Aspect" in avro_schema.other_props
    ):
        # probably an aspect schema
        record_schema: avro.schema.RecordSchema = avro_schema
        aspect_def = record_schema.get_prop("Aspect")
        aspect_definition = AspectDefinition(**aspect_def)

        aspect_definition.schema = record_schema
        aspect_registry[aspect_definition.name] = aspect_definition
        logger.debug(f"Loaded aspect schema: {aspect_definition.name}")
    elif avro_schema.name == "MetadataChangeEvent":
        # probably an MCE schema
        field: Field = avro_schema.fields[1]
        assert isinstance(field.type, avro.schema.UnionSchema)
        for member_schema in field.type.schemas:
            if "Entity" in member_schema.props:
                entity_def = member_schema.props["Entity"]
                entity_name = entity_def["name"]
                entity_definition = entity_registry.get(
                    entity_name, EntityDefinition(**entity_def)
                )
                entity_definition.aspect_map = get_aspects_from_snapshot(member_schema)
                all_aspects = [a for a in entity_definition.aspect_map]
                # in terms of order, we prefer the aspects from snapshot over the aspects from the config registry
                # so we flip the aspect list here
                for aspect_name in entity_definition.aspects:
                    if aspect_name not in all_aspects:
                        all_aspects.append(aspect_name)
                entity_definition.aspects = all_aspects
                entity_registry[entity_name] = entity_definition
                logger.debug(
                    f"Loaded entity schema: {entity_name} with aspects: {all_aspects}"
                )
    else:
        logger.debug(f"Ignoring schema {schema_file}")


def extract_lineage_fields_from_schema(
    schema: avro.schema.Schema, current_path: str = ""
) -> List[LineageField]:
    """
    Recursively extract lineage fields from an Avro schema.

    Args:
        schema: The Avro schema to analyze
        current_path: The current field path (for nested fields)

    Returns:
        List of LineageField objects found in the schema
    """
    lineage_fields = []

    if isinstance(schema, avro.schema.RecordSchema):
        logger.debug(f"Analyzing record schema at path: {current_path}")
        for field in schema.fields:
            field_path = f"{current_path}.{field.name}" if current_path else field.name
            logger.debug(f"Analyzing field: {field.name} at path: {field_path}")

            # Check if this field has lineage properties
            is_lineage = False
            relationship_info = None

            # Check for isLineage property
            if hasattr(field, "other_props") and field.other_props:
                is_lineage = field.other_props.get("isLineage", False)
                if is_lineage:
                    logger.debug(f"Found isLineage=true for field: {field_path}")

                # Check for Relationship property
                if "Relationship" in field.other_props:
                    relationship_data = field.other_props["Relationship"]
                    logger.debug(
                        f"Found Relationship property for field: {field_path}: {relationship_data}"
                    )

                    # Handle both direct relationship and path-based relationship
                    if "entityTypes" in relationship_data:
                        # Direct relationship
                        relationship_is_lineage = relationship_data.get(
                            "isLineage", False
                        )
                        relationship_info = LineageRelationship(
                            name=relationship_data.get("name", ""),
                            entityTypes=relationship_data.get("entityTypes", []),
                            isLineage=relationship_is_lineage,
                        )
                        is_lineage = is_lineage or relationship_is_lineage
                    else:
                        # Path-based relationship - find the actual relationship data
                        for _, value in relationship_data.items():
                            if isinstance(value, dict) and "entityTypes" in value:
                                relationship_is_lineage = value.get("isLineage", False)
                                relationship_info = LineageRelationship(
                                    name=value.get("name", ""),
                                    entityTypes=value.get("entityTypes", []),
                                    isLineage=relationship_is_lineage,
                                )
                                is_lineage = is_lineage or relationship_is_lineage
                                break

            # If this field is lineage, add it to the results
            if is_lineage:
                lineage_field = LineageField(
                    name=field.name,
                    path=field_path,
                    isLineage=True,
                    relationship=relationship_info,
                )
                lineage_fields.append(lineage_field)
                logger.debug(f"Added lineage field: {field_path}")

            # Recursively check nested fields
            nested_fields = extract_lineage_fields_from_schema(field.type, field_path)
            lineage_fields.extend(nested_fields)

    elif isinstance(schema, avro.schema.ArraySchema):
        logger.debug(f"Analyzing array schema at path: {current_path}")
        # For arrays, check the items schema
        nested_fields = extract_lineage_fields_from_schema(schema.items, current_path)
        lineage_fields.extend(nested_fields)

    elif isinstance(schema, avro.schema.UnionSchema):
        logger.debug(f"Analyzing union schema at path: {current_path}")
        # For unions, check all possible schemas
        for union_schema in schema.schemas:
            nested_fields = extract_lineage_fields_from_schema(
                union_schema, current_path
            )
            lineage_fields.extend(nested_fields)

    return lineage_fields


def extract_lineage_fields() -> LineageData:
    """
    Extract lineage fields from all aspects in the aspect registry.

    Returns:
        LineageData containing all lineage information organized by entity and aspect
    """
    logger.info("Starting lineage field extraction")

    lineage_data = LineageData(entities={})

    # Group aspects by entity
    entity_aspects: Dict[str, List[str]] = {}
    for entity_name, entity_def in entity_registry.items():
        entity_aspects[entity_name] = entity_def.aspects
        logger.debug(f"Entity {entity_name} has aspects: {entity_def.aspects}")

    # Process each aspect
    for aspect_name, aspect_def in aspect_registry.items():
        logger.debug(f"Processing aspect: {aspect_name}")

        if not aspect_def.schema:
            logger.warning(f"Aspect {aspect_name} has no schema, skipping")
            continue

        # Extract lineage fields from this aspect
        lineage_fields = extract_lineage_fields_from_schema(aspect_def.schema)

        if lineage_fields:
            logger.info(
                f"Found {len(lineage_fields)} lineage fields in aspect {aspect_name}"
            )

            # Find which entities use this aspect
            for entity_name, entity_aspect_list in entity_aspects.items():
                if aspect_name in entity_aspect_list:
                    logger.debug(
                        f"Aspect {aspect_name} is used by entity {entity_name}"
                    )

                    # Initialize entity if not exists
                    if entity_name not in lineage_data.entities:
                        lineage_data.entities[entity_name] = LineageEntity(aspects={})

                    # Add aspect with lineage fields
                    lineage_aspect = LineageAspect(
                        aspect=aspect_name, fields=lineage_fields
                    )
                    lineage_data.entities[entity_name].aspects[aspect_name] = (
                        lineage_aspect
                    )
        else:
            logger.debug(f"No lineage fields found in aspect {aspect_name}")

    # Log summary
    total_entities_with_lineage = len(lineage_data.entities)
    total_aspects_with_lineage = sum(
        len(entity.aspects) for entity in lineage_data.entities.values()
    )
    total_lineage_fields = sum(
        len(aspect.fields)
        for entity in lineage_data.entities.values()
        for aspect in entity.aspects.values()
    )

    logger.info("Lineage extraction complete:")
    logger.info(f"  - Entities with lineage: {total_entities_with_lineage}")
    logger.info(f"  - Aspects with lineage: {total_aspects_with_lineage}")
    logger.info(f"  - Total lineage fields: {total_lineage_fields}")

    return lineage_data


def generate_lineage_json(lineage_data: LineageData) -> str:
    """
    Generate JSON representation of lineage data.

    Args:
        lineage_data: The lineage data to convert to JSON

    Returns:
        JSON string representation
    """
    logger.info("Generating lineage JSON")

    # Convert dataclasses to dictionaries
    def lineage_field_to_dict(field: LineageField) -> Dict[str, Any]:
        result = {"name": field.name, "path": field.path, "isLineage": field.isLineage}
        if field.relationship:
            result["relationship"] = {
                "name": field.relationship.name,
                "entityTypes": field.relationship.entityTypes,
                "isLineage": field.relationship.isLineage,
            }
        return result

    def lineage_aspect_to_dict(aspect: LineageAspect) -> Dict[str, Any]:
        return {
            "aspect": aspect.aspect,
            "fields": [lineage_field_to_dict(field) for field in aspect.fields],
        }

    def lineage_entity_to_dict(entity: LineageEntity) -> Dict[str, Any]:
        return {
            aspect_name: lineage_aspect_to_dict(aspect)
            for aspect_name, aspect in entity.aspects.items()
        }

    # Build the final JSON structure
    json_data = {
        "entities": {
            entity_name: lineage_entity_to_dict(entity)
            for entity_name, entity in lineage_data.entities.items()
        }
    }

    json_data["generated_by"] = "metadata-ingestion/scripts/modeldocgen.py"
    json_data["generated_at"] = datetime.now(timezone.utc).isoformat()

    json_string = json.dumps(json_data, indent=2)
    logger.info(f"Generated lineage JSON with {len(json_string)} characters")

    return json_string


@dataclass
class Relationship:
    name: str
    src: str
    dst: str
    doc: Optional[str] = None
    id: Optional[str] = None


@dataclass
class RelationshipAdjacency:
    self_loop: List[Relationship] = field(default_factory=list)
    incoming: List[Relationship] = field(default_factory=list)
    outgoing: List[Relationship] = field(default_factory=list)


@dataclass
class RelationshipGraph:
    map: Dict[str, RelationshipAdjacency] = field(default_factory=dict)

    def add_edge(
        self, src: str, dst: str, label: str, reason: str, edge_id: Optional[str] = None
    ) -> None:
        relnship = Relationship(
            label, src, dst, reason, id=edge_id or f"{src}:{label}:{dst}:{reason}"
        )

        if src == dst:
            adjacency = self.map.get(src, RelationshipAdjacency())
            for reln in adjacency.self_loop:
                if relnship.id == reln.id:
                    print(f"Skipping adding edge since ids match {reln.id}")
                    return
            adjacency.self_loop.append(relnship)
            self.map[src] = adjacency
        else:
            adjacency = self.map.get(src, RelationshipAdjacency())
            for reln in adjacency.outgoing:
                if relnship.id == reln.id:
                    logger.info(f"Skipping adding edge since ids match {reln.id}")
                    return

            adjacency.outgoing.append(relnship)
            self.map[src] = adjacency

            adjacency = self.map.get(dst, RelationshipAdjacency())
            for reln in adjacency.incoming:
                if relnship.id == reln.id:
                    logger.info(f"Skipping adding edge since ids match {reln.id}")
                    return

            adjacency.incoming.append(relnship)
            self.map[dst] = adjacency

    def get_adjacency(self, node: str) -> RelationshipAdjacency:
        return self.map.get(node, RelationshipAdjacency())


def make_relnship_docs(relationships: List[Relationship], direction: str) -> str:
    doc = ""
    map: Dict[str, List[Relationship]] = {}
    for relnship in relationships:
        map[relnship.name] = map.get(relnship.name, [])
        map[relnship.name].append(relnship)
    for rel_name, relnships in map.items():
        doc += f"\n- {rel_name}\n"
        for relnship in relnships:
            doc += f"\n   - {relnship.dst if direction == 'outgoing' else relnship.src}{relnship.doc or ''}"
    return doc


def expand_inline_directives(content: str) -> str:
    """
    Expand {{ inline /path/to/file }} directives in markdown content.

    This replicates the behavior of docs-website/generateDocsDir.ts's
    markdown_process_inline_directives function.

    Args:
        content: Markdown content potentially containing inline directives

    Returns:
        Content with inline directives expanded
    """

    def replace_inline(match: re.Match) -> str:
        indent = match.group(1)
        inline_file_path = match.group(3)

        if not inline_file_path.startswith("/"):
            raise ValueError(f"inline path must be absolute: {inline_file_path}")

        # Read file from repo root (one level up from metadata-ingestion/scripts)
        repo_root = Path(__file__).parent.parent.parent
        referenced_file_path = repo_root / inline_file_path.lstrip("/")

        if not referenced_file_path.exists():
            logger.warning(f"Inline file not found: {inline_file_path}")
            return match.group(0)

        logger.debug(f"Inlining {inline_file_path}")
        referenced_content = referenced_file_path.read_text()

        # Add indentation to each line (don't add "Inlined from" comment - files already have path comments)
        new_content = "\n".join(
            f"{indent}{line}" for line in referenced_content.split("\n")
        )

        return new_content

    # Pattern matches: {{ inline /path/to/file }} or {{ inline /path/to/file show_path_as_comment }}
    pattern = r"^(\s*){{(\s*)inline\s+(\S+)(\s+)(show_path_as_comment\s+)?(\s*)}}$"
    return re.sub(pattern, replace_inline, content, flags=re.MULTILINE)


def convert_relative_links_to_absolute(content: str) -> str:
    """
    Convert relative markdown links to absolute docs.datahub.com URLs.

    Relative links don't work properly in DataHub UI, so we convert them to absolute
    links pointing to the public documentation site.

    Args:
        content: Markdown content with potential relative links

    Returns:
        Content with relative .md links converted to absolute URLs
    """

    def replace_link(match: re.Match) -> str:
        link_text = match.group(1)
        link_url = match.group(2)

        # Skip if already absolute
        if link_url.startswith("http://") or link_url.startswith("https://"):
            return match.group(0)

        # Parse anchor (e.g., #section)
        parts = link_url.split("#")
        path = parts[0]
        anchor = "#" + parts[1] if len(parts) > 1 else ""

        # Remove .md extension
        if path.endswith(".md"):
            path = path[:-3]

        # Resolve path to absolute form
        if path.startswith("/docs/"):
            # Already absolute doc path - use as-is
            resolved = path
        elif path.startswith("./"):
            # Same directory (entities) - remove ./
            resolved = "/docs/generated/metamodel/entities/" + path[2:]
        elif path.startswith("../"):
            # Relative path from entities directory
            # We're at /docs/generated/metamodel/entities/
            # Count how many levels to go up
            up_count = 0
            temp_path = path
            while temp_path.startswith("../"):
                up_count += 1
                temp_path = temp_path[3:]

            # Start from entities path and go up
            base_parts = ["docs", "generated", "metamodel", "entities"]
            base_parts = base_parts[: len(base_parts) - up_count]

            # Add remaining path
            remaining_parts = temp_path.split("/") if temp_path else []
            resolved_parts = base_parts + remaining_parts
            resolved = "/" + "/".join(resolved_parts)
        elif "/" in path:
            # Path with directory but no prefix - assume it needs /docs/
            resolved = "/" + path if path.startswith("docs/") else "/docs/" + path
        else:
            # Just a filename - assume same directory (entities)
            resolved = "/docs/generated/metamodel/entities/" + path

        # Build absolute URL (lowercase for docs.datahub.com compatibility)
        absolute_url = f"https://docs.datahub.com{resolved.lower()}{anchor}"

        return f"[{link_text}]({absolute_url})"

    # Match markdown links with .md extension: [text](path.md) or [text](path.md#anchor)
    pattern = r"\[([^\]]+)\]\(([^)]+\.md[^)]*)\)"
    return re.sub(pattern, replace_link, content)


@dataclass
class FieldInfo:
    name: str
    type_name: str
    is_array: bool
    is_optional: bool
    description: str
    annotations: List[str]


def simplify_type_name(field_type: Any) -> Tuple[str, bool, bool]:
    """
    Extract simplified type name, whether it's an array, and whether it's optional.
    Returns: (type_name, is_array, is_optional)
    """
    is_optional = False
    is_array = False
    current_type = field_type

    # Handle union types (optional fields)
    if isinstance(current_type, avro.schema.UnionSchema):
        non_null_types = [t for t in current_type.schemas if t.type != "null"]
        if len(non_null_types) == 1:
            is_optional = True
            current_type = non_null_types[0]
        else:
            # Multiple non-null types - just show as union
            return "union", False, True

    # Handle array types
    if isinstance(current_type, avro.schema.ArraySchema):
        is_array = True
        current_type = current_type.items

    # Get the actual type name
    if isinstance(current_type, avro.schema.RecordSchema):
        type_name = current_type.name
    elif isinstance(current_type, avro.schema.PrimitiveSchema):
        type_name = current_type.type
    elif isinstance(current_type, avro.schema.MapSchema):
        type_name = "map"
    elif isinstance(current_type, avro.schema.EnumSchema):
        type_name = current_type.name
    else:
        type_name = (
            str(current_type.type) if hasattr(current_type, "type") else "unknown"
        )

    return type_name, is_array, is_optional


def extract_fields_from_schema(schema: avro.schema.RecordSchema) -> List[FieldInfo]:
    """Extract top-level fields from an Avro schema."""
    fields = []

    for avro_field in schema.fields:
        type_name, is_array, is_optional = simplify_type_name(avro_field.type)

        # Extract annotations
        annotations = []

        # Check for deprecated field
        if hasattr(avro_field, "other_props") and avro_field.other_props:
            if avro_field.other_props.get("deprecated"):
                annotations.append("⚠️ Deprecated")

        if hasattr(avro_field, "other_props") and avro_field.other_props:
            if "Searchable" in avro_field.other_props:
                searchable_config = avro_field.other_props["Searchable"]
                # Check if there's a custom search field name
                if (
                    isinstance(searchable_config, dict)
                    and "fieldName" in searchable_config
                ):
                    search_field_name = searchable_config["fieldName"]
                    # Only show override if it differs from actual field name
                    if search_field_name != avro_field.name:
                        annotations.append(f"Searchable ({search_field_name})")
                    else:
                        annotations.append("Searchable")
                else:
                    annotations.append("Searchable")
            if "Relationship" in avro_field.other_props:
                relationship_info = avro_field.other_props["Relationship"]
                # Extract relationship name if available
                rel_name = None
                if isinstance(relationship_info, dict):
                    if "name" in relationship_info:
                        rel_name = relationship_info.get("name")
                    else:
                        # Path-based relationship
                        for _, v in relationship_info.items():
                            if isinstance(v, dict) and "name" in v:
                                rel_name = v.get("name")
                                break
                if rel_name:
                    annotations.append(f"→ {rel_name}")

        # Get field description
        description = (
            avro_field.doc if hasattr(avro_field, "doc") and avro_field.doc else ""
        )

        fields.append(
            FieldInfo(
                name=avro_field.name,
                type_name=type_name,
                is_array=is_array,
                is_optional=is_optional,
                description=description,
                annotations=annotations,
            )
        )

    return fields


def generate_field_table(fields: List[FieldInfo], common_types: set) -> str:
    """Generate markdown table for fields with hyperlinks to common types."""
    table = "\n| Field | Type | Required | Description | Annotations |\n"
    table += "|-------|------|----------|-------------|-------------|\n"

    for field_info in fields:
        # Format type with optional hyperlink
        type_display = field_info.type_name
        if field_info.type_name in common_types:
            # Create anchor link (lowercase, replace spaces with hyphens)
            anchor = field_info.type_name.lower().replace(" ", "-")
            type_display = f"[{field_info.type_name}](#{anchor})"

        if field_info.is_array:
            type_display += "[]"

        # Required/optional
        required = "✓" if not field_info.is_optional else ""

        # Annotations
        notes = ", ".join(field_info.annotations) if field_info.annotations else ""

        # Clean description (remove newlines, truncate if too long)
        desc = field_info.description.replace("\n", " ").strip()
        if len(desc) > 100:
            desc = desc[:97] + "..."

        table += (
            f"| {field_info.name} | {type_display} | {required} | {desc} | {notes} |\n"
        )

    return table


def identify_common_types(
    entity_def: EntityDefinition,
) -> Dict[str, avro.schema.RecordSchema]:
    """
    Identify types that appear in multiple aspects and should be documented separately.
    Returns a dict of type_name -> schema for common types.
    """
    type_usage: Dict[str, Tuple[avro.schema.RecordSchema, int]] = {}

    # Track which types appear and how often
    for aspect_name in entity_def.aspects or []:
        if aspect_name not in aspect_registry:
            continue
        aspect_def = aspect_registry[aspect_name]
        if not aspect_def.schema:
            continue

        # Walk through fields and collect record types
        for avro_field in aspect_def.schema.fields:
            _collect_record_types(avro_field.type, type_usage)

    # Common types are those that appear more than once OR are well-known types
    well_known_types = {
        "AuditStamp",
        "Edge",
        "ChangeAuditStamps",
        "TimeStamp",
        "Urn",
        "DataPlatformInstance",
    }

    common_types = {}
    for type_name, (schema, count) in type_usage.items():
        if count > 1 or type_name in well_known_types:
            common_types[type_name] = schema

    return common_types


def _collect_record_types(
    field_type: Any, type_usage: Dict[str, Tuple[avro.schema.RecordSchema, int]]
) -> None:
    """Recursively collect record types from a field type."""
    if isinstance(field_type, avro.schema.UnionSchema):
        for union_type in field_type.schemas:
            _collect_record_types(union_type, type_usage)
    elif isinstance(field_type, avro.schema.ArraySchema):
        _collect_record_types(field_type.items, type_usage)
    elif isinstance(field_type, avro.schema.RecordSchema):
        type_name = field_type.name
        if type_name in type_usage:
            schema, count = type_usage[type_name]
            type_usage[type_name] = (schema, count + 1)
        else:
            type_usage[type_name] = (field_type, 1)


def generate_common_types_section(
    common_types: Dict[str, avro.schema.RecordSchema],
) -> str:
    """Generate the Common Types reference section."""
    if not common_types:
        return ""

    section = "\n### Common Types\n\n"
    section += "These types are used across multiple aspects in this entity.\n\n"

    for type_name in sorted(common_types.keys()):
        schema = common_types[type_name]
        section += f"#### {type_name}\n\n"

        doc = schema.get_prop("doc") if hasattr(schema, "get_prop") else None
        if doc:
            section += f"{doc}\n\n"

        # Show fields of this common type
        section += "**Fields:**\n\n"
        for avro_field in schema.fields:
            type_name_inner, is_array, is_optional = simplify_type_name(avro_field.type)
            type_display = type_name_inner
            if is_array:
                type_display += "[]"
            optional_marker = "?" if is_optional else ""

            field_doc = (
                avro_field.doc if hasattr(avro_field, "doc") and avro_field.doc else ""
            )
            field_doc = field_doc.replace("\n", " ").strip()
            if len(field_doc) > 80:
                field_doc = field_doc[:77] + "..."

            section += f"- `{avro_field.name}` ({type_display}{optional_marker}): {field_doc}\n"

        section += "\n"

    return section


def make_entity_docs(entity_display_name: str, graph: RelationshipGraph) -> str:
    entity_name = entity_display_name[0:1].lower() + entity_display_name[1:]
    entity_def: Optional[EntityDefinition] = entity_registry.get(entity_name)
    if entity_def:
        doc = entity_def.doc_file_contents or (
            f"# {entity_def.display_name}\n{entity_def.doc}"
            if entity_def.doc
            else f"# {entity_def.display_name}"
        )

        # Identify common types for this entity
        common_types_map = identify_common_types(entity_def)
        common_type_names = set(common_types_map.keys())

        # Add Tabs import at the start
        tabs_import = (
            "\nimport Tabs from '@theme/Tabs';\nimport TabItem from '@theme/TabItem';\n"
        )

        # create aspects section
        aspects_section = "\n### Aspects\n" if entity_def.aspects else ""

        deprecated_aspects_section = ""
        timeseries_aspects_section = ""

        for aspect in entity_def.aspects or []:
            aspect_definition: AspectDefinition = aspect_registry[aspect]
            assert aspect_definition
            assert aspect_definition.schema
            deprecated_message = (
                " (Deprecated)"
                if aspect_definition.schema.get_prop("Deprecated")
                else ""
            )
            timeseries_qualifier = (
                " (Timeseries)" if aspect_definition.type == "timeseries" else ""
            )

            # Generate aspect documentation with tabs
            this_aspect_doc = ""
            this_aspect_doc += (
                f"\n#### {aspect}{deprecated_message}{timeseries_qualifier}\n"
            )
            this_aspect_doc += f"{aspect_definition.schema.get_prop('doc')}\n\n"

            # Extract fields for table view
            fields = extract_fields_from_schema(aspect_definition.schema)

            # Generate tabbed interface
            this_aspect_doc += "<Tabs>\n"
            this_aspect_doc += '<TabItem value="fields" label="Fields" default>\n\n'
            this_aspect_doc += generate_field_table(fields, common_type_names)
            this_aspect_doc += "\n</TabItem>\n"
            this_aspect_doc += '<TabItem value="raw" label="Raw Schema">\n\n'
            this_aspect_doc += f"```javascript\n{json.dumps(aspect_definition.schema.to_json(), indent=2)}\n```\n"
            this_aspect_doc += "\n</TabItem>\n"
            this_aspect_doc += "</Tabs>\n\n"

            if deprecated_message:
                deprecated_aspects_section += this_aspect_doc
            elif timeseries_qualifier:
                timeseries_aspects_section += this_aspect_doc
            else:
                aspects_section += this_aspect_doc

        aspects_section += timeseries_aspects_section + deprecated_aspects_section

        # Generate common types section
        common_types_section = generate_common_types_section(common_types_map)

        # create relationships section
        relationships_section = "\n### Relationships\n"
        adjacency = graph.get_adjacency(entity_def.display_name)
        if adjacency.self_loop:
            relationships_section += "\n#### Self\nThese are the relationships to itself, stored in this entity's aspects"
        for relnship in adjacency.self_loop:
            relationships_section += (
                f"\n- {relnship.name} ({relnship.doc[1:] if relnship.doc else ''})"
            )

        if adjacency.outgoing:
            relationships_section += "\n#### Outgoing\nThese are the relationships stored in this entity's aspects"
            relationships_section += make_relnship_docs(
                adjacency.outgoing, direction="outgoing"
            )

        if adjacency.incoming:
            relationships_section += "\n#### Incoming\nThese are the relationships stored in other entity's aspects"
            relationships_section += make_relnship_docs(
                adjacency.incoming, direction="incoming"
            )

        # create global metadata graph
        global_graph_url = "https://github.com/datahub-project/static-assets/raw/main/imgs/datahub-metadata-model.png"
        global_graph_section = (
            f"\n### [Global Metadata Model]({global_graph_url})"
            + f"\n![Global Graph]({global_graph_url})"
        )

        # create technical reference guide section
        technical_reference_section = "\n## Technical Reference Guide\n\n"
        technical_reference_section += (
            "The sections above provide an overview of how to use this entity. "
            "The following sections provide detailed technical information about how metadata is stored and represented in DataHub.\n\n"
        )
        technical_reference_section += (
            "**Aspects** are the individual pieces of metadata that can be attached to an entity. "
            "Each aspect contains specific information (like ownership, tags, or properties) and is stored as a separate record, "
            "allowing for flexible and incremental metadata updates.\n\n"
        )
        technical_reference_section += (
            "**Relationships** show how this entity connects to other entities in the metadata graph. "
            "These connections are derived from the fields within each aspect and form the foundation of DataHub's knowledge graph.\n\n"
        )
        technical_reference_section += (
            "### Reading the Field Tables\n\n"
            "Each aspect's field table includes an **Annotations** column that provides additional metadata about how fields are used:\n\n"
            "- **⚠️ Deprecated**: This field is deprecated and may be removed in a future version. Check the description for the recommended alternative\n"
            "- **Searchable**: This field is indexed and can be searched in DataHub's search interface\n"
            "- **Searchable (fieldname)**: When the field name in parentheses is shown, it indicates the field is indexed under a different name in the search index. For example, `dashboardTool` is indexed as `tool`\n"
            "- **→ RelationshipName**: This field creates a relationship to another entity. The arrow indicates this field contains a reference (URN) to another entity, and the name indicates the type of relationship (e.g., `→ Contains`, `→ OwnedBy`)\n\n"
            "Fields with complex types (like `Edge`, `AuditStamp`) link to their definitions in the [Common Types](#common-types) section below.\n"
        )

        final_doc = (
            doc
            + tabs_import
            + technical_reference_section
            + aspects_section
            + common_types_section
            + relationships_section
            + global_graph_section
        )
        generated_documentation[entity_name] = final_doc
        return final_doc
    else:
        raise Exception(f"Failed to find information for entity: {entity_name}")


def make_entity_docs_datahub(entity_display_name: str) -> str:
    """
    Generate DataHub-optimized documentation for an entity.

    This variant is simpler than the Docusaurus version:
    - Expands {{ inline ... }} directives (since DataHub won't process them)
    - Converts relative markdown links to absolute docs.datahub.com URLs
    - Excludes technical sections (already available in DataHub's Columns tab)
    - Removes Docusaurus-specific components (Tabs, TabItem, etc.)

    Args:
        entity_display_name: Display name of the entity (e.g., "Dataset")

    Returns:
        Simplified documentation suitable for DataHub ingestion
    """
    entity_name = entity_display_name[0:1].lower() + entity_display_name[1:]
    entity_def: Optional[EntityDefinition] = entity_registry.get(entity_name)

    if not entity_def:
        raise Exception(f"Failed to find information for entity: {entity_name}")

    # Get main documentation content (from file or from entity definition)
    if entity_def.doc_file_contents:
        doc = entity_def.doc_file_contents
    elif entity_def.doc:
        doc = f"# {entity_def.display_name}\n{entity_def.doc}"
    else:
        doc = f"# {entity_def.display_name}"

    # Expand {{ inline ... }} directives
    doc = expand_inline_directives(doc)

    # Convert relative markdown links to absolute URLs
    doc = convert_relative_links_to_absolute(doc)

    # Add pointer to DataHub's Columns tab for technical details
    technical_pointer = (
        "\n\n## Technical Reference\n\n"
        "For technical details about fields, searchability, and relationships, "
        "view the **Columns** tab in DataHub.\n"
    )

    final_doc = doc + technical_pointer

    generated_documentation_datahub[entity_name] = final_doc
    return final_doc


def create_search_field_name_property() -> List[MetadataChangeProposalWrapper]:
    """
    Create the structured property for documenting search field names.

    This property is used to capture the actual field name used in the search index
    when it differs from the field name in the schema (e.g., 'instance' field is
    indexed as 'platformInstance').

    Returns:
        List of MCPs for the property definition and settings
    """
    property_id = "com.datahub.metadata.searchFieldName"
    property_urn = str(
        StructuredPropertyUrn.from_string(f"urn:li:structuredProperty:{property_id}")
    )

    # Create property definition
    definition_mcp = MetadataChangeProposalWrapper(
        entityUrn=property_urn,
        aspect=StructuredPropertyDefinitionClass(
            qualifiedName=property_id,
            displayName="Search Field Name",
            valueType=Urn.make_data_type_urn("string"),
            description=(
                "The field name used in the search index when it differs from the schema field name. "
                "Use this field name when constructing search queries for this field."
            ),
            entityTypes=[Urn.make_entity_type_urn("schemaField")],
            cardinality="SINGLE",
            immutable=False,
        ),
    )

    # Create property settings for display
    settings_mcp = MetadataChangeProposalWrapper(
        entityUrn=property_urn,
        aspect=StructuredPropertySettingsClass(
            isHidden=False,
            showInSearchFilters=False,
            showInAssetSummary=True,
            showAsAssetBadge=False,
            showInColumnsTable=True,  # Show as a column in schema tables
        ),
    )

    return [definition_mcp, settings_mcp]


def generate_stitched_record(
    relnships_graph: RelationshipGraph,
) -> Iterable[MetadataChangeProposalWrapper]:
    def strip_types(field_path: str) -> str:
        final_path = field_path
        final_path = re.sub(r"(\[type=[a-zA-Z]+\]\.)", "", final_path)
        final_path = re.sub(r"^\[version=2.0\]\.", "", final_path)
        return final_path

    # Track schema fields that need structured properties
    schema_field_properties: Dict[
        str, str
    ] = {}  # schema_field_urn -> search_field_name

    for entity_name, entity_def in entity_registry.items():
        entity_display_name = entity_def.display_name
        entity_fields = []
        for aspect_name in entity_def.aspects:
            if aspect_name not in aspect_registry:
                print(f"Did not find aspect name: {aspect_name} in aspect_registry")
                continue

            # all aspects should have a schema
            aspect_schema = aspect_registry[aspect_name].schema
            assert aspect_schema
            entity_fields.append(
                {
                    "type": aspect_schema.to_json(),
                    "name": aspect_name,
                }
            )

        if entity_fields:
            names = avro.schema.Names()
            field_objects = []
            for f in entity_fields:
                field = avro.schema.Field(
                    f["type"],
                    f["name"],
                    has_default=False,
                )
                field_objects.append(field)

            with unittest.mock.patch("avro.schema.Names.add_name", add_name):
                entity_avro_schema = avro.schema.RecordSchema(
                    name=entity_name,
                    namespace="datahub.metadata.model",
                    names=names,
                    fields=[],
                )
                entity_avro_schema.set_prop("fields", field_objects)
            rawSchema = json.dumps(entity_avro_schema.to_json())
            # always add the URN which is the primary key
            urn_field = SchemaField(
                fieldPath="urn",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
                nullable=False,
                isPartOfKey=True,
                description=f"The primary identifier for the {entity_name} entity. See the {entity_def.keyAspect} field to understand the structure of this urn.",
            )
            schema_fields: List[SchemaField] = [urn_field] + avro_schema_to_mce_fields(
                rawSchema
            )
            foreign_keys: List[ForeignKeyConstraintClass] = []
            source_dataset_urn = make_dataset_urn(
                platform="datahub",
                name=f"{entity_display_name}",
            )
            for f_field in schema_fields:
                if f_field.jsonProps:
                    json_dict = json.loads(f_field.jsonProps)
                    if "Aspect" in json_dict:
                        aspect_info = json_dict["Aspect"]
                        f_field.globalTags = f_field.globalTags or GlobalTagsClass(
                            tags=[]
                        )
                        f_field.globalTags.tags.append(
                            TagAssociationClass(tag="urn:li:tag:Aspect")
                        )
                        # if this is the key aspect, also add primary-key
                        if entity_def.keyAspect == aspect_info.get("name"):
                            f_field.isPartOfKey = True

                        if aspect_info.get("type", "") == "timeseries":
                            # f_field.globalTags = f_field.globalTags or GlobalTagsClass(
                            #    tags=[]
                            # )
                            f_field.globalTags.tags.append(
                                TagAssociationClass(tag="urn:li:tag:Temporal")
                            )
                    if "Searchable" in json_dict:
                        f_field.globalTags = f_field.globalTags or GlobalTagsClass(
                            tags=[]
                        )
                        f_field.globalTags.tags.append(
                            TagAssociationClass(tag="urn:li:tag:Searchable")
                        )

                        # Check if search field name differs from actual field name
                        searchable_config = json_dict["Searchable"]
                        if (
                            isinstance(searchable_config, dict)
                            and "fieldName" in searchable_config
                        ):
                            search_field_name = searchable_config["fieldName"]
                            # Extract the actual field name from the field path
                            # Field path format: "[version=2.0].[type=...].<fieldName>"
                            actual_field_name = strip_types(f_field.fieldPath).split(
                                "."
                            )[-1]

                            if search_field_name != actual_field_name:
                                # Track this for later - we'll emit a separate MCP for the schema field entity
                                schema_field_urn = make_schema_field_urn(
                                    source_dataset_urn, f_field.fieldPath
                                )
                                schema_field_properties[schema_field_urn] = (
                                    search_field_name
                                )
                    if "Relationship" in json_dict:
                        relationship_info = json_dict["Relationship"]
                        # detect if we have relationship specified at leaf level or thru path specs
                        if "entityTypes" not in relationship_info:
                            # path spec
                            assert len(relationship_info.keys()) == 1, (
                                "We should never have more than one path spec assigned to a relationship annotation"
                            )
                            final_info = None
                            for _, v in relationship_info.items():
                                final_info = v
                            relationship_info = final_info

                        assert "entityTypes" in relationship_info

                        entity_types: List[str] = relationship_info.get(
                            "entityTypes", []
                        )
                        relnship_name = relationship_info.get("name", None)
                        for entity_type in entity_types:
                            destination_entity_name = capitalize_first(entity_type)

                            foreign_dataset_urn = make_dataset_urn(
                                platform="datahub",
                                name=destination_entity_name,
                            )
                            fkey = ForeignKeyConstraintClass(
                                name=relnship_name,
                                foreignDataset=foreign_dataset_urn,
                                foreignFields=[
                                    make_schema_field_urn(foreign_dataset_urn, "urn")
                                ],
                                sourceFields=[
                                    make_schema_field_urn(
                                        source_dataset_urn, f_field.fieldPath
                                    )
                                ],
                            )
                            foreign_keys.append(fkey)
                            relnships_graph.add_edge(
                                entity_display_name,
                                destination_entity_name,
                                fkey.name,
                                f" via `{strip_types(f_field.fieldPath)}`",
                                edge_id=f"{entity_display_name}:{fkey.name}:{destination_entity_name}:{strip_types(f_field.fieldPath)}",
                            )

            dataset_urn = make_dataset_urn(
                platform="datahub",
                name=entity_display_name,
            )

            yield from MetadataChangeProposalWrapper.construct_many(
                entityUrn=dataset_urn,
                aspects=[
                    SchemaMetadataClass(
                        schemaName=str(entity_name),
                        platform=make_data_platform_urn("datahub"),
                        platformSchema=OtherSchemaClass(rawSchema=rawSchema),
                        fields=schema_fields,
                        version=0,
                        hash="",
                        foreignKeys=foreign_keys if foreign_keys else None,
                    ),
                    GlobalTagsClass(
                        tags=[TagAssociationClass(tag="urn:li:tag:Entity")]
                    ),
                    BrowsePathsClass([f"/prod/datahub/entities/{entity_display_name}"]),
                    BrowsePathsV2Class(
                        [
                            BrowsePathEntryClass(id="entities"),
                            BrowsePathEntryClass(id=entity_display_name),
                        ]
                    ),
                    DatasetPropertiesClass(
                        description=(
                            # Generate Docusaurus docs (stores in generated_documentation)
                            make_entity_docs(entity_display_name, relnships_graph),
                            # But use DataHub variant for ingestion
                            generated_documentation_datahub.get(
                                entity_name, f"# {entity_display_name}"
                            ),
                        )[1]  # Select the DataHub variant
                    ),
                    SubTypesClass(typeNames=["entity"]),
                ],
            )

    # Emit structured properties for schema fields
    property_urn = "urn:li:structuredProperty:com.datahub.metadata.searchFieldName"
    for schema_field_urn, search_field_name in schema_field_properties.items():
        yield MetadataChangeProposalWrapper(
            entityUrn=schema_field_urn,
            aspect=StructuredPropertiesClass(
                properties=[
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn=property_urn,
                        values=[search_field_name],
                    )
                ]
            ),
        )


@dataclass
class EntityAspectName:
    entityName: str
    aspectName: str


class AspectPluginConfig(PermissiveConfigModel):
    className: str
    enabled: bool
    supportedEntityAspectNames: List[EntityAspectName] = []
    packageScan: Optional[List[str]] = None
    supportedOperations: Optional[List[str]] = None


class PluginConfiguration(PermissiveConfigModel):
    aspectPayloadValidators: Optional[List[AspectPluginConfig]] = None
    mutationHooks: Optional[List[AspectPluginConfig]] = None
    mclSideEffects: Optional[List[AspectPluginConfig]] = None
    mcpSideEffects: Optional[List[AspectPluginConfig]] = None


class EntityRegistry(PermissiveConfigModel):
    entities: List[EntityDefinition]
    events: Optional[List[EventDefinition]]
    plugins: Optional[PluginConfiguration] = None


def load_registry_file(registry_file: str) -> Dict[str, EntityDefinition]:
    import yaml

    with open(registry_file, "r") as f:
        registry = EntityRegistry.parse_obj(yaml.safe_load(f))
        for index, entity_def in enumerate(registry.entities):
            entity_def.priority = index
            entity_registry[entity_def.name] = entity_def
    return entity_registry


def get_sorted_entity_names(
    entity_names: List[Tuple[str, EntityDefinition]],
) -> List[Tuple[str, List[str]]]:
    """
    Sort entity names by category and priority for documentation generation.

    This function organizes entities into a structured order for generating
    documentation. Entities are grouped by category (CORE vs INTERNAL) and
    within each category, sorted by priority and then alphabetically.

    Business Logic:
    - CORE entities are displayed first, followed by INTERNAL entities
    - Within each category, entities with priority values are sorted first
    - Priority entities are sorted by their priority value (lower numbers = higher priority)
    - Non-priority entities are sorted alphabetically after priority entities
    - Zero and negative priority values are treated as valid priorities

    Args:
        entity_names: List of tuples containing (entity_name, EntityDefinition)

    Returns:
        List of tuples containing (EntityCategory, List[str]) where:
        - First tuple: (EntityCategory.CORE, sorted_core_entity_names)
        - Second tuple: (EntityCategory.INTERNAL, sorted_internal_entity_names)

    Example:
        Input: [
            ("dataset", EntityDefinition(priority=2, category=CORE)),
            ("table", EntityDefinition(priority=None, category=CORE)),
            ("internal", EntityDefinition(priority=1, category=INTERNAL))
        ]
        Output: [
            (EntityCategory.CORE, ["dataset", "table"]),
            (EntityCategory.INTERNAL, ["internal"])
        ]
    """
    core_entities = [
        (x, y) for (x, y) in entity_names if y.category == EntityCategory.CORE
    ]
    priority_bearing_core_entities = [(x, y) for (x, y) in core_entities if y.priority]
    priority_bearing_core_entities.sort(key=lambda t: t[1].priority)
    priority_bearing_core_entities = [x for (x, y) in priority_bearing_core_entities]

    non_priority_core_entities = [x for (x, y) in core_entities if not y.priority]
    non_priority_core_entities.sort()

    internal_entities = [
        (x, y) for (x, y) in entity_names if y.category == EntityCategory.INTERNAL
    ]
    priority_bearing_internal_entities = [
        x for (x, y) in internal_entities if y.priority
    ]

    non_priority_internal_entities = [
        x for (x, y) in internal_entities if not y.priority
    ]

    sorted_entities = [
        (
            EntityCategory.CORE,
            priority_bearing_core_entities + non_priority_core_entities,
        ),
        (
            EntityCategory.INTERNAL,
            priority_bearing_internal_entities + non_priority_internal_entities,
        ),
    ]

    return sorted_entities


@click.command()
@click.argument("schemas_root", type=click.Path(exists=True), required=True)
@click.option("--registry", type=click.Path(exists=True), required=True)
@click.option("--generated-docs-dir", type=click.Path(exists=True), required=True)
@click.option("--server", type=str, required=False)
@click.option("--file", type=str, required=False)
@click.option(
    "--dot", type=str, required=False, help="generate a dot file representing the graph"
)
@click.option("--png", type=str, required=False)
@click.option("--extra-docs", type=str, required=False)
@click.option(
    "--lineage-output", type=str, required=False, help="generate lineage JSON file"
)
def generate(  # noqa: C901
    schemas_root: str,
    registry: str,
    generated_docs_dir: str,
    server: Optional[str],
    file: Optional[str],
    dot: Optional[str],
    png: Optional[str],
    extra_docs: Optional[str],
    lineage_output: Optional[str],
) -> None:
    logger.info(f"server = {server}")
    logger.info(f"file = {file}")
    logger.info(f"dot = {dot}")
    logger.info(f"png = {png}")
    logger.info(f"lineage_output = {lineage_output}")

    entity_extra_docs = {}
    if extra_docs:
        for path in glob.glob(f"{extra_docs}/**/*.md", recursive=True):
            m = re.search("/docs/entities/(.*)/*.md", path)
            if m:
                entity_name = m.group(1)
                with open(path, "r") as doc_file:
                    file_contents = doc_file.read()
                    entity_extra_docs[entity_name] = file_contents

    # registry file
    load_registry_file(registry)

    # schema files
    for schema_file in Path(schemas_root).glob("**/*.avsc"):
        if (
            schema_file.name in {"MetadataChangeEvent.avsc"}
            or json.loads(schema_file.read_text()).get("Aspect") is not None
        ):
            load_schema_file(str(schema_file))

    if entity_extra_docs:
        for entity_name in entity_extra_docs:
            entity_registry[entity_name].doc_file_contents = entity_extra_docs[
                entity_name
            ]

    if lineage_output:
        logger.info(f"Generating lineage JSON to {lineage_output}")
        try:
            lineage_data = extract_lineage_fields()
            lineage_json = generate_lineage_json(lineage_data)

            output_path = Path(lineage_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            new_json_data = json.loads(lineage_json)

            write_file = should_write_json_file(
                output_path, new_json_data, "lineage file"
            )

            if write_file:
                with open(output_path, "w") as f:
                    f.write(lineage_json)
                logger.info(f"Successfully wrote lineage JSON to {lineage_output}")

        except Exception as e:
            logger.error(f"Failed to generate lineage JSON: {e}")
            raise

    # Create structured property for search field names first
    logger.info("Creating structured property for search field names")
    structured_property_mcps = create_search_field_name_property()

    # Generate DataHub-optimized documentation for all entities
    # This must be done before generate_stitched_record() so the docs are available for MCPs
    logger.info("Generating DataHub-optimized entity documentation")
    for entity_name in entity_registry:
        entity_def = entity_registry[entity_name]
        try:
            make_entity_docs_datahub(entity_def.display_name)
        except Exception as e:
            logger.warning(f"Failed to generate DataHub docs for {entity_name}: {e}")

    relationship_graph = RelationshipGraph()
    entity_mcps = list(generate_stitched_record(relationship_graph))

    # Combine MCPs with structured property first
    mcps = structured_property_mcps + entity_mcps

    shutil.rmtree(f"{generated_docs_dir}/entities", ignore_errors=True)
    entity_names = [(x, entity_registry[x]) for x in generated_documentation]

    sorted_entity_names = get_sorted_entity_names(entity_names)

    index = 0
    for _, sorted_entities in sorted_entity_names:
        for entity_name in sorted_entities:
            entity_dir = f"{generated_docs_dir}/entities/"

            os.makedirs(entity_dir, exist_ok=True)

            # Write Docusaurus variant (with Tabs, technical sections, etc.)
            with open(f"{entity_dir}/{entity_name}.md", "w") as fp:
                fp.write("---\n")
                fp.write(f"sidebar_position: {index}\n")
                fp.write("---\n")
                fp.write(generated_documentation[entity_name])

            # Write DataHub variant (simplified, with inline directives expanded)
            if entity_name in generated_documentation_datahub:
                with open(f"{entity_dir}/{entity_name}-datahub.md", "w") as fp:
                    fp.write("---\n")
                    fp.write(f"sidebar_position: {index}\n")
                    fp.write("---\n")
                    fp.write(generated_documentation_datahub[entity_name])

            index += 1

    if file:
        logger.info(f"Will write events to {file}")
        Path(file).parent.mkdir(parents=True, exist_ok=True)
        fileSink = FileSink(
            PipelineContext(run_id="generated-metaModel"),
            FileSinkConfig(filename=file),
        )
        for e in mcps:
            fileSink.write_record_async(
                RecordEnvelope(e, metadata={}), write_callback=NoopWriteCallback()
            )
        fileSink.close()
        pipeline_config = {
            "source": {
                "type": "file",
                "config": {"filename": file},
            },
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": "${DATAHUB_SERVER:-http://localhost:8080}",
                    "token": "${DATAHUB_TOKEN:-}",
                },
            },
            "run_id": "modeldoc-generated",
        }
        pipeline_file = Path(file).parent.absolute() / "pipeline.yml"
        with open(pipeline_file, "w") as f:
            json.dump(pipeline_config, f, indent=2)
            logger.info(f"Wrote pipeline to {pipeline_file}")

    if server:
        logger.info(f"Will send events to {server}")
        assert server.startswith("http://"), "server address must start with http://"
        emitter = DatahubRestEmitter(gms_server=server)
        emitter.test_connection()
        for e in mcps:
            emitter.emit(e)

    if dot:
        logger.info(f"Will write dot file to {dot}")

        import pydot

        graph = pydot.Dot("my_graph", graph_type="graph")
        for node, adjacency in relationship_graph.map.items():
            my_node = pydot.Node(
                node,
                label=node,
                shape="box",
            )
            graph.add_node(my_node)
            if adjacency.self_loop:
                for relnship in adjacency.self_loop:
                    graph.add_edge(
                        pydot.Edge(
                            src=relnship.src, dst=relnship.dst, label=relnship.name
                        )
                    )
            if adjacency.outgoing:
                for relnship in adjacency.outgoing:
                    graph.add_edge(
                        pydot.Edge(
                            src=relnship.src, dst=relnship.dst, label=relnship.name
                        )
                    )
        Path(dot).parent.mkdir(parents=True, exist_ok=True)
        graph.write_raw(dot)
        if png:
            try:
                graph.write_png(png)
            except Exception as e:
                logger.error(
                    "Failed to create png file. Do you have graphviz installed?"
                )
                raise e


if __name__ == "__main__":
    logger.setLevel("INFO")
    generate()
