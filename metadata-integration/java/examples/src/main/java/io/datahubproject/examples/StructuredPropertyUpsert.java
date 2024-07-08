package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayMap;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertyDefinitionPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class StructuredPropertyUpsert {

  private StructuredPropertyUpsert() {}

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    // open ended string structured property on datasets and dataFlows
    MetadataChangeProposal mcp1 =
        new StructuredPropertyDefinitionPatchBuilder()
            .urn(
                UrnUtils.getUrn(
                    "urn:li:structuredProperty:testString")) // use existing urn for update, new urn
            // for new property
            .setQualifiedName("io.acryl.testString")
            .setDisplayName("Open Ended String")
            .setValueType("urn:li:dataType:datahub.string")
            .setCardinality(PropertyCardinality.SINGLE)
            .addEntityType("urn:li:entityType:datahub.dataset")
            .addEntityType("urn:li:entityType:datahub.dataFlow")
            .setDescription("test description for open ended string")
            .setImmutable(true)
            .build();

    String token = "";
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));
    Future<MetadataWriteResponse> response1 = emitter.emit(mcp1, null);
    System.out.println(response1.get().getResponseContent());

    // Next, let's make a property that allows for multiple datahub entity urns as values
    // This example property could be used to reference other users or groups in datahub
    StringArrayMap typeQualifier = new StringArrayMap();
    typeQualifier.put(
        "allowedTypes",
        new StringArray(
            "urn:li:entityType:datahub.corpuser", "urn:li:entityType:datahub.corpGroup"));

    MetadataChangeProposal mcp2 =
        new StructuredPropertyDefinitionPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:structuredProperty:dataSteward"))
            .setQualifiedName("io.acryl.dataManagement.dataSteward")
            .setDisplayName("Data Steward")
            .setValueType("urn:li:dataType:datahub.urn")
            .setTypeQualifier(typeQualifier)
            .setCardinality(PropertyCardinality.MULTIPLE)
            .addEntityType("urn:li:entityType:datahub.dataset")
            .setDescription(
                "The data stewards of this asset are in charge of ensuring data cleanliness and governance")
            .setImmutable(true)
            .build();

    Future<MetadataWriteResponse> response2 = emitter.emit(mcp2, null);
    System.out.println(response2.get().getResponseContent());

    // Finally, let's make a single select number property with a few allowed options
    PropertyValue propertyValue1 = new PropertyValue();
    PrimitivePropertyValue value1 = new PrimitivePropertyValue();
    value1.setDouble(30.0);
    propertyValue1.setDescription(
        "30 days, usually reserved for datasets that are ephemeral and contain pii");
    propertyValue1.setValue(value1);
    PropertyValue propertyValue2 = new PropertyValue();
    PrimitivePropertyValue value2 = new PrimitivePropertyValue();
    value2.setDouble(90.0);
    propertyValue2.setDescription(
        "Use this for datasets that drive monthly reporting but contain pii");
    propertyValue2.setValue(value2);

    MetadataChangeProposal mcp3 =
        new StructuredPropertyDefinitionPatchBuilder()
            .urn(UrnUtils.getUrn("urn:li:structuredProperty:replicationSLA"))
            .setQualifiedName("io.acryl.dataManagement.replicationSLA")
            .setDisplayName("Replication SLA")
            .setValueType("urn:li:dataType:datahub.string")
            .addAllowedValue(propertyValue1)
            .addAllowedValue(propertyValue2)
            .setCardinality(PropertyCardinality.SINGLE)
            .addEntityType("urn:li:entityType:datahub.dataset")
            .setDescription(
                "SLA for how long data can be delayed before replicating to the destination cluster")
            .setImmutable(false)
            .build();

    Future<MetadataWriteResponse> response3 = emitter.emit(mcp3, null);
    System.out.println(response3.get().getResponseContent());
  }
}
