import logging
import uuid
from typing import Dict, Iterable, List

from avrogen.dict_wrapper import DictWrapper

from datahub.cli import cli_utils
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ChartInfoClass,
    DataJobInputOutputClass,
    DataProcessInfoClass,
    MLFeaturePropertiesClass,
    MLPrimaryKeyPropertiesClass,
    SchemaMetadataClass,
    SystemMetadataClass,
    UpstreamLineageClass,
)

log = logging.getLogger(__name__)


def get_aspect_name_from_relationship_type_and_entity(
    relationship_type: str, entity_type: str
) -> str:
    if relationship_type == "Produces":
        if entity_type.lower() == "datajob":
            return "dataJobInputOutput"
    if relationship_type == "Consumes":
        if entity_type.lower() == "chart":
            return "chartInfo"
        if entity_type.lower() == "datajob":
            return "dataJobInputOutput"
        if entity_type.lower() == "dataProcess":
            return "dataProcessInfo"
    if relationship_type == "DownstreamOf":
        if entity_type.lower() == "dataset":
            return "upstreamLineage"
    if relationship_type == "ForeignKeyToDataset":
        if entity_type.lower() == "dataset":
            return "schemaMetadata"
    if relationship_type == "DerivedFrom":
        if entity_type.lower() == "mlfeature":
            return "mlFeatureProperties"
        if entity_type.lower() == "mlprimarykey":
            return "mlPrimaryKeyProperties"
    raise Exception(
        f"Unable to map aspect name from relationship_type {relationship_type} and entity_type {entity_type}"
    )


def modify_urn_list_for_aspect(
    aspect_name: str,
    aspect: DictWrapper,
    relationship_type: str,
    old_urn: str,
    new_urn: str,
) -> DictWrapper:
    if aspect_name == "dataJobInputOutput":
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
    if aspect_name == "chartInfo":
        assert isinstance(aspect, ChartInfoClass)
        chartInfo: ChartInfoClass = aspect
        chartInfo.inputs = [
            new_urn if x == old_urn else x for x in chartInfo.inputs or []
        ]
        return chartInfo
    if aspect_name == "dataProcessInfo":
        assert isinstance(aspect, DataProcessInfoClass)
        dataProcessInfo: DataProcessInfoClass = aspect
        if dataProcessInfo.inputs is not None:
            dataProcessInfo.inputs = [
                new_urn if x == old_urn else x for x in dataProcessInfo.inputs
            ]
        return dataProcessInfo
    if aspect_name == "upstreamLineage":
        assert isinstance(aspect, UpstreamLineageClass)
        upstreamLineage: UpstreamLineageClass = aspect
        for upstream in upstreamLineage.upstreams:
            if upstream.dataset == old_urn:
                upstream.dataset = new_urn
        return upstreamLineage
    if aspect_name == "schemaMetadata":
        assert isinstance(aspect, SchemaMetadataClass)
        schemaMetadata: SchemaMetadataClass = aspect
        for foreignKey in schemaMetadata.foreignKeys or []:
            foreignKey.foreignFields = [
                f.replace(old_urn, new_urn) for f in foreignKey.foreignFields
            ]
            if foreignKey.foreignDataset == old_urn:
                foreignKey.foreignDataset = new_urn
        return schemaMetadata
    if aspect_name == "mlFeatureProperties":
        assert isinstance(aspect, MLFeaturePropertiesClass)
        mlFeatureProperties: MLFeaturePropertiesClass = aspect
        mlFeatureProperties.sources = [
            new_urn if source == old_urn else source
            for source in mlFeatureProperties.sources or []
        ]
        return mlFeatureProperties
    if aspect_name == "mlPrimaryKeyProperties":
        assert isinstance(aspect, MLPrimaryKeyPropertiesClass)
        ml_pk_aspect: MLPrimaryKeyPropertiesClass = aspect
        ml_pk_aspect.sources = [
            new_urn if source == old_urn else source for source in ml_pk_aspect.sources
        ]
        return ml_pk_aspect
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

    aspect_map = cli_utils.get_aspects_for_entity(
        entity_urn=src_urn, aspects=aspect_names, typed=True
    )
    if aspect_names is not None:
        for a in aspect_names:
            if a in aspect_map:
                aspect_value = aspect_map[a]
                assert isinstance(aspect_value, DictWrapper)
                new_mcp = MetadataChangeProposalWrapper(
                    entityUrn=dst_urn,
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName=a,
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


def get_incoming_relationships_dataset(urn: str) -> Iterable[Dict]:
    yield from cli_utils.get_incoming_relationships(
        urn,
        types=[
            "DownstreamOf",
            "Consumes",
            "Produces",
            "ForeignKeyToDataset",
            "DerivedFrom",
        ],
    )
