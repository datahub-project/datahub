import json
import logging
import unittest.mock
from dataclasses import Field, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import avro.schema
import click

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChangeTypeClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    ForeignKeyConstraintClass,
    GlobalTagsClass,
    MetadataChangeEventClass,
    OtherSchemaClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    SystemMetadataClass,
    TagAssociationClass,
)

logger = logging.getLogger(__name__)


def capitalize_first(something: str) -> str:
    return something[0:1].upper() + something[1:]


@dataclass
class EntityDefinition:
    name: str
    keyAspect: str
    aspects: List[str] = field(default_factory=list)
    aspect_map: Optional[Dict[str, Any]] = None
    relationship_map: Optional[Dict[str, str]] = None
    doc: Optional[str] = None
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


# Patch add_name method to NOT complain about duplicate names
def add_name(self, name_attr, space_attr, new_schema):
    to_add = avro.schema.Name(name_attr, space_attr, self.default_namespace)

    if self.names:
        self.names[to_add.fullname] = new_schema
    return to_add


def load_schema_file(schema_file: str) -> None:

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
        try:
            aspect_definition = AspectDefinition(**aspect_def)
        except Exception as e:
            import pdb

            breakpoint()

        aspect_definition.schema = record_schema
        aspect_registry[aspect_definition.name] = aspect_definition
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
                all_aspects = [a for a in entity_definition.aspect_map.keys()]
                # in terms of order, we prefer the aspects from snapshot over the aspects from the config registry
                # so we flip the aspect list here
                for aspect_name in entity_definition.aspects:
                    if aspect_name not in all_aspects:
                        all_aspects.append(aspect_name)
                entity_definition.aspects = all_aspects
                entity_registry[entity_name] = entity_definition
    else:
        print(f"Ignoring schema {schema_file}")


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


def make_entity_docs(entity_display_name: str, graph: RelationshipGraph) -> str:
    entity_name = entity_display_name[0:1].lower() + entity_display_name[1:]
    entity_def: Optional[EntityDefinition] = entity_registry.get(entity_name, None)
    if entity_def:
        import pdb

        # breakpoint()
        doc = entity_def.doc or f"This is the {entity_def.display_name} entity."
        # create relationships section
        relationships_section = f"\n## Relationships\n"
        adjacency = graph.get_adjacency(entity_def.display_name)
        if adjacency.self_loop:
            relationships_section += f"\n### Self\nThese are the relationships to itself, stored in this entity's aspects"
        for relnship in adjacency.self_loop:
            relationships_section += f"\n- {relnship.name} ({relnship.doc[1:] if relnship.doc else ''})"

        if adjacency.outgoing:
            relationships_section += f"\n### Outgoing\nThese are the relationships stored in this entity's aspects"
            relationships_section += make_relnship_docs(
                adjacency.outgoing, direction="outgoing"
            )

        if adjacency.incoming:
            relationships_section += f"\n### Incoming\nThese are the relationships stored in other entity's aspects"
            relationships_section += make_relnship_docs(
                adjacency.incoming, direction="incoming"
            )

        # create global metadata graph
        global_graph_url = "https://github.com/linkedin/datahub/raw/master/docs/imgs/datahub-metadata-model.png"
        global_graph_section = (
            f"\n## [Global Metadata Model]({global_graph_url})"
            + f"\n![Global Graph]({global_graph_url})"
        )
        return doc + relationships_section + global_graph_section
    else:
        raise Exception(f"Failed to find information for entity: {entity_name}")


def generate_stitched_record(relnships_graph: RelationshipGraph) -> List[Any]:
    def strip_types(field_path: str) -> str:
        import re

        final_path = field_path
        final_path = re.sub(r"(\[type=[a-zA-Z]+\]\.)", "", final_path)
        final_path = re.sub(r"^\[version=2.0\]\.", "", final_path)
        return final_path

    datasets: List[DatasetSnapshotClass] = []

    for entity_name, entity_def in entity_registry.items():
        entity_display_name = entity_def.display_name
        entity_fields = []
        for aspect_name in entity_def.aspects:
            if aspect_name not in aspect_registry:
                print(f"Did not find aspect name: {aspect_name} in aspect_registry")
                continue
            import pdb

            # breakpoint()
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
                    type_=f["type"],
                    name=f["name"],
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
                platform=make_data_platform_urn("datahub"),
                name=f"{entity_display_name}",
            )
            for f_field in schema_fields:
                if f_field.jsonProps:
                    import pdb
                    #breakpoint()
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

                        if "timeseries" == aspect_info.get("type", ""):
                            # f_field.globalTags = f_field.globalTags or GlobalTagsClass(
                            #    tags=[]
                            # )
                            f_field.globalTags.tags.append(
                                TagAssociationClass(tag="urn:li:tag:Temporal")
                            )
                        import pdb

                        # breakpoint()
                    if "Searchable" in json_dict:
                        f_field.globalTags = f_field.globalTags or GlobalTagsClass(
                            tags=[]
                        )
                        f_field.globalTags.tags.append(
                            TagAssociationClass(tag="urn:li:tag:Searchable")
                        )
                    if "Relationship" in json_dict:
                        relationship_info = json_dict["Relationship"]
                        # detect if we have relationship specified at leaf level or thru path specs
                        if "entityTypes" not in relationship_info:
                            # path spec
                            assert (
                                len(relationship_info.keys()) == 1
                            ), "We should never have more than one path spec assigned to a relationship annotation"
                            final_info = None
                            for k, v in relationship_info.items():
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
                                platform=make_data_platform_urn("datahub"),
                                name=destination_entity_name,
                            )
                            fkey = ForeignKeyConstraintClass(
                                name=relnship_name,
                                foreignDataset=foreign_dataset_urn,
                                foreignFields=[
                                    f"urn:li:schemaField:({foreign_dataset_urn}, urn)"
                                ],
                                sourceFields=[
                                    f"urn:li:schemaField:({source_dataset_urn},{f_field.fieldPath})"
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

            schemaMetadata = SchemaMetadataClass(
                schemaName=f"{entity_name}",
                platform=make_data_platform_urn("datahub"),
                platformSchema=OtherSchemaClass(rawSchema=rawSchema),
                fields=schema_fields,
                version=0,
                hash="",
                foreignKeys=foreign_keys if foreign_keys else None,
            )

            dataset = DatasetSnapshotClass(
                urn=make_dataset_urn(
                    platform=make_data_platform_urn("datahub"),
                    name=f"{entity_display_name}",
                ),
                aspects=[
                    schemaMetadata,
                    GlobalTagsClass(
                        tags=[TagAssociationClass(tag="urn:li:tag:Entity")]
                    ),
                    BrowsePathsClass([f"/prod/datahub/entities/{entity_display_name}"]),
                ],
            )
            datasets.append(dataset)

    events: List[Union[MetadataChangeEventClass, MetadataChangeProposalWrapper]] = []

    for d in datasets:
        entity_name = d.urn.split(":")[-1].split(",")[1]
        d.aspects.append(
            DatasetPropertiesClass(
                description=make_entity_docs(entity_name, relnships_graph)
            )
        )

        mce = MetadataChangeEventClass(
            proposedSnapshot=d,
        )
        events.append(mce)

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=d.urn,
            aspectName="subTypes",
            aspect=SubTypesClass(typeNames=["entity"]),
        )
        events.append(mcp)
    return events


from datahub.configuration.common import ConfigModel


class EntityRegistry(ConfigModel):
    entities: List[EntityDefinition]


def load_registry_file(registry_file: str) -> Dict[str, EntityDefinition]:
    import yaml

    with open(registry_file, "r") as f:
        registry = EntityRegistry.parse_obj(yaml.safe_load(f))
        for entity_def in registry.entities:
            entity_registry[entity_def.name] = entity_def

    return entity_registry


@click.command()
@click.argument("schema_files", type=click.Path(exists=True), nargs=-1, required=True)
@click.option("--server", type=str, required=False)
@click.option("--file", type=str, required=False)
@click.option("--dot", type=str, required=False)
@click.option("--png", type=str, required=False)
def generate(
    schema_files: List[str],
    server: Optional[str],
    file: Optional[str],
    dot: Optional[str],
    png: Optional[str]
) -> None:
    logger.info(f"server = {server}")
    logger.info(f"file = {file}")
    logger.info(f"dot = {dot}")
    logger.info(f"png = {png}")

    for schema_file in schema_files:
        if schema_file.endswith(".yml") or schema_file.endswith(".yaml"):
            # registry file
            load_registry_file(schema_file)
        else:
            # schema file
            load_schema_file(schema_file)
            
    relationship_graph = RelationshipGraph()
    events = generate_stitched_record(relationship_graph)

    if file:
        logger.info(f"Will write events to {file}")
        Path(file).parent.mkdir(parents=True, exist_ok=True)
        fileSink = FileSink(
            PipelineContext(run_id="generated-metaModel"),
            FileSinkConfig(filename=file),
        )
        for e in events:
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
        for e in events:
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
                logger.error("Failed to create png file. Do you have graphviz installed?")
                raise e

if __name__ == "__main__":
    logger.setLevel("INFO")
    generate()
