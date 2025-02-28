package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertiesPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DatasetStructuredPropertiesUpdate {

  private DatasetStructuredPropertiesUpdate() {}

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {

    // Adding a structured property with a single string value
    MetadataChangeProposal mcp1 =
        new StructuredPropertiesPatchBuilder()
            .urn(
                UrnUtils.getUrn(
                    "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)"))
            .setStringProperty(
                UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"), "30")
            .build();

    String token = "";
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));
    Future<MetadataWriteResponse> response1 = emitter.emit(mcp1, null);
    System.out.println(response1.get().getResponseContent());

    // Adding a structured property with a list of string values
    List<String> values = new ArrayList<>();
    values.add("30");
    values.add("90");
    MetadataChangeProposal mcp2 =
        new StructuredPropertiesPatchBuilder()
            .urn(
                UrnUtils.getUrn(
                    "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)"))
            .setStringProperty(
                UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.privacy.retentionTime"), values)
            .build();

    Future<MetadataWriteResponse> response2 = emitter.emit(mcp2, null);
    System.out.println(response2.get().getResponseContent());

    // Adding a structured property with a single number value
    MetadataChangeProposal mcp3 =
        new StructuredPropertiesPatchBuilder()
            .urn(
                UrnUtils.getUrn(
                    "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)"))
            .setNumberProperty(
                UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.dataManagement.replicationSLA"),
                3456)
            .build();

    Future<MetadataWriteResponse> response3 = emitter.emit(mcp3, null);
    System.out.println(response3.get().getResponseContent());

    // Removing a structured property from a dataset
    MetadataChangeProposal mcp4 =
        new StructuredPropertiesPatchBuilder()
            .urn(
                UrnUtils.getUrn(
                    "urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD)"))
            .removeProperty(
                UrnUtils.getUrn("urn:li:structuredProperty:io.acryl.dataManagement.replicationSLA"))
            .build();

    Future<MetadataWriteResponse> response4 = emitter.emit(mcp4, null);
    System.out.println(response4.get().getResponseContent());
  }
}
