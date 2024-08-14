import logging
import uuid
from typing import Iterable, List

from avrogen.dict_wrapper import DictWrapper

from datahub.cli import cli_utils
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import (
    DataHubGraph,
    RelatedEntity,
    get_default_graph,
)
from datahub.metadata.schema_classes import (
    ChartInfoClass,
    ContainerClass,
    DataJobInputOutputClass,
    DataProcessInfoClass,
    MLFeaturePropertiesClass,
    MLPrimaryKeyPropertiesClass,
    SchemaMetadataClass,
    SystemMetadataClass,
    UpstreamLineageClass,
)

log = logging.getLogger(__name__)

# TODO: Make this dynamic based on the real aspect class map.
all_aspects = [
    "schemaMetadata",
    "datasetProperties",
    "viewProperties",
    "subTypes",
    "editableDatasetProperties",
    "ownership",
    "datasetDeprecation",
    "institutionalMemory",
    "editableSchemaMetadata",
    "globalTags",
    "glossaryTerms",
    "upstreamLineage",
    "datasetUpstreamLineage",
    "status",
    "containerProperties",
    "dataPlatformInstance",
    "containerKey",
    "container",
    "domains",
    "containerProperties",
    "editableContainerProperties",
]


def get_aspect_name_from_relationship(relationship_type: str, entity_type: str) -> str:
    aspect_map = {
        "Produces": {"datajob": "dataJobInputOutput"},
        "Consumes": {
            "chart": "chartInfo",
            "datajob": "dataJobInputOutput",
            "dataProcess": "dataProcessInfo",
        },
        "DownstreamOf": {"dataset": "upstreamLineage"},
        "ForeignKeyToDataset": {"dataset": "schemaMetadata"},
        "DerivedFrom": {
            "mlfeature": "mlFeatureProperties",
            "mlprimarykey": "mlPrimaryKeyProperties",
        },
        "IsPartOf": {
            "container": "container",
            "dataset": "container",
            "dashboard": "container",
            "chart": "container",
        },
    }

    if (
        relationship_type in aspect_map
        and entity_type.lower() in aspect_map[relationship_type]
    ):
        return aspect_map[relationship_type][entity_type.lower()]

    raise Exception(
        f"Unable to map aspect name from relationship_type {relationship_type} and entity_type {entity_type}"
    )


class UrnListModifier:
    @staticmethod
    def dataJobInputOutput_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, DataJobInputOutputClass)
        dataJobInputOutput: DataJobInputOutputClass = aspect
        if relationship_type == "Produces":
            dataJobInputOutput.outputDatasets = [
                new_urn if dataset == old_urn else dataset
                for dataset in dataJobInputOutput.outputDatasets
            ]
            return dataJobInputOutput
        if relationship_type == "Consumes":
            dataJobInputOutput.inputDatasets = [
                new_urn if dataset == old_urn else dataset
                for dataset in dataJobInputOutput.inputDatasets
            ]
            return dataJobInputOutput

        raise Exception(
            f"Unable to map aspect_name: dataJobInputOutput, relationship_type {relationship_type}"
        )

    @staticmethod
    def chartInfo_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, ChartInfoClass)
        chartInfo: ChartInfoClass = aspect
        chartInfo.inputs = [
            new_urn if x == old_urn else x for x in chartInfo.inputs or []
        ]
        return chartInfo

    @staticmethod
    def dataProcessInfo_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, DataProcessInfoClass)
        dataProcessInfo: DataProcessInfoClass = aspect
        if dataProcessInfo.inputs is not None:
            dataProcessInfo.inputs = [
                new_urn if x == old_urn else x for x in dataProcessInfo.inputs
            ]
        return dataProcessInfo

    @staticmethod
    def upstreamLineage_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, UpstreamLineageClass)
        upstreamLineage: UpstreamLineageClass = aspect
        for upstream in upstreamLineage.upstreams:
            if upstream.dataset == old_urn:
                upstream.dataset = new_urn
        return upstreamLineage

    @staticmethod
    def schemaMetadata_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, SchemaMetadataClass)
        schemaMetadata: SchemaMetadataClass = aspect
        for foreignKey in schemaMetadata.foreignKeys or []:
            foreignKey.foreignFields = [
                f.replace(old_urn, new_urn) for f in foreignKey.foreignFields
            ]
            if foreignKey.foreignDataset == old_urn:
                foreignKey.foreignDataset = new_urn
        return schemaMetadata

    @staticmethod
    def mlFeatureProperties_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, MLFeaturePropertiesClass)
        mlFeatureProperties: MLFeaturePropertiesClass = aspect
        mlFeatureProperties.sources = [
            new_urn if source == old_urn else source
            for source in mlFeatureProperties.sources or []
        ]
        return mlFeatureProperties

    @staticmethod
    def mlPrimaryKeyProperties_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, MLPrimaryKeyPropertiesClass)
        ml_pk_aspect: MLPrimaryKeyPropertiesClass = aspect
        ml_pk_aspect.sources = [
            new_urn if source == old_urn else source for source in ml_pk_aspect.sources
        ]
        return ml_pk_aspect

    @staticmethod
    def container_modifier(
        aspect: DictWrapper,
        relationship_type: str,
        old_urn: str,
        new_urn: str,
    ) -> DictWrapper:
        assert isinstance(aspect, ContainerClass)
        container: ContainerClass = aspect
        container.container = (
            new_urn if container.container == old_urn else container.container
        )
        return container


def modify_urn_list_for_aspect(
    aspect_name: str,
    aspect: Aspect,
    relationship_type: str,
    old_urn: str,
    new_urn: str,
) -> Aspect:
    if hasattr(UrnListModifier, f"{aspect_name}_modifier"):
        modifier = getattr(UrnListModifier, f"{aspect_name}_modifier")
        return modifier(
            aspect=aspect,
            relationship_type=relationship_type,
            old_urn=old_urn,
            new_urn=new_urn,
        )
    raise Exception(
        f"Unable to map aspect_name: {aspect_name}, relationship_type {relationship_type}"
    )


def clone_aspect(
    src_urn: str,
    aspect_names: List[str],
    dst_urn: str,
    run_id: str = str(uuid.uuid4()),
    dry_run: bool = False,
) -> Iterable[MetadataChangeProposalWrapper]:
    client = get_default_graph()
    aspect_map = cli_utils.get_aspects_for_entity(
        client._session,
        client.config.server,
        entity_urn=src_urn,
        aspects=aspect_names,
        typed=True,
    )

    if aspect_names is not None:
        for a in aspect_names:
            if a in aspect_map:
                aspect_value = aspect_map[a]
                assert isinstance(aspect_value, DictWrapper)
                new_mcp = MetadataChangeProposalWrapper(
                    entityUrn=dst_urn,
                    aspect=aspect_value,
                    systemMetadata=SystemMetadataClass(
                        runId=run_id,
                    ),
                )
                if not dry_run:
                    log.debug(f"Emitting mcp for {dst_urn}")
                    yield new_mcp
                else:
                    log.debug(f"Would update aspect {a} as {aspect_map[a]}")
            else:
                log.debug(f"did not find aspect {a} in response, continuing...")


def get_incoming_relationships(urn: str) -> Iterable[RelatedEntity]:
    client = get_default_graph()
    yield from client.get_related_entities(
        entity_urn=urn,
        relationship_types=[
            "DownstreamOf",
            "Consumes",
            "Produces",
            "ForeignKeyToDataset",
            "DerivedFrom",
            "IsPartOf",
        ],
        direction=DataHubGraph.RelationshipDirection.INCOMING,
    )


def get_outgoing_relationships(urn: str) -> Iterable[RelatedEntity]:
    client = get_default_graph()
    yield from client.get_related_entities(
        entity_urn=urn,
        relationship_types=[
            "DownstreamOf",
            "Consumes",
            "Produces",
            "ForeignKeyToDataset",
            "DerivedFrom",
            "IsPartOf",
        ],
        direction=DataHubGraph.RelationshipDirection.OUTGOING,
    )
