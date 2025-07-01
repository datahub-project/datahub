import logging

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    PropertyValueClass,
    StructuredPropertyDefinitionClass,
)
from datahub.metadata.urns import StructuredPropertyUrn

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server="http://localhost:8080")

# first, let's make an open ended structured property that allows one text value
text_property_urn = StructuredPropertyUrn("io.acryl.openTextProperty")
text_property_definition = StructuredPropertyDefinitionClass(
    qualifiedName="io.acryl.openTextProperty",
    displayName="Open Text Property",
    valueType="urn:li:dataType:datahub.string",
    cardinality="SINGLE",
    entityTypes=[
        "urn:li:entityType:datahub.dataset",
        "urn:li:entityType:datahub.container",
    ],
    description="This structured property allows a signle open ended response as a value",
    immutable=False,
)

event_prop_1: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=str(text_property_urn),
    aspect=text_property_definition,
)
rest_emitter.emit(event_prop_1)

# next, let's make a property that allows for multiple datahub entity urns as values
# This example property could be used to reference other users or groups in datahub
urn_property_urn = StructuredPropertyUrn("io.acryl.dataManagement.dataSteward")
urn_property_definition = StructuredPropertyDefinitionClass(
    qualifiedName="io.acryl.dataManagement.dataSteward",
    displayName="Data Steward",
    valueType="urn:li:dataType:datahub.urn",
    cardinality="MULTIPLE",
    entityTypes=["urn:li:entityType:datahub.dataset"],
    description="The data stewards of this asset are in charge of ensuring data cleanliness and governance",
    immutable=True,
    typeQualifier={
        "allowedTypes": [
            "urn:li:entityType:datahub.corpuser",
            "urn:li:entityType:datahub.corpGroup",
        ]
    },  # this line ensures only user or group urns can be assigned as values
)

event_prop_2: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=str(urn_property_urn),
    aspect=urn_property_definition,
)
rest_emitter.emit(event_prop_2)

# finally, let's make a single select number property with a few allowed options
number_property_urn = StructuredPropertyUrn("io.acryl.dataManagement.replicationSLA")
number_property_definition = StructuredPropertyDefinitionClass(
    qualifiedName="io.acryl.dataManagement.replicationSLA",
    displayName="Retention Time",
    valueType="urn:li:dataType:datahub.number",
    cardinality="SINGLE",
    entityTypes=[
        "urn:li:entityType:datahub.dataset",
        "urn:li:entityType:datahub.dataFlow",
    ],
    description="SLA for how long data can be delayed before replicating to the destination cluster",
    immutable=False,
    allowedValues=[
        PropertyValueClass(
            value=30,
            description="30 days, usually reserved for datasets that are ephemeral and contain pii",
        ),
        PropertyValueClass(
            value=90,
            description="Use this for datasets that drive monthly reporting but contain pii",
        ),
        PropertyValueClass(
            value=365,
            description="Use this for non-sensitive data that can be retained for longer",
        ),
    ],
)

event_prop_3: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=str(number_property_urn),
    aspect=number_property_definition,
)
rest_emitter.emit(event_prop_3)
