package io.datahubproject.examples;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.builder.DataJobInputOutputPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DataJobLineageAdd {

  private DataJobLineageAdd() {}

  /**
   * Adds lineage to an existing DataJob without affecting any lineage
   *
   * @param args
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    String token = "";
    try (RestEmitter emitter =
        RestEmitter.create(b -> b.server("http://localhost:8080").token(token))) {
      MetadataChangeProposal dataJobIOPatch =
          new DataJobInputOutputPatchBuilder()
              .urn(
                  UrnUtils.getUrn(
                      "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_456)"))
              .addInputDatasetEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleKafkaDataset,PROD)"))
              .addOutputDatasetEdge(
                  DatasetUrn.createFromString(
                      "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleHiveDataset,PROD)"))
              .addInputDatajobEdge(
                  DataJobUrn.createFromString(
                      "urn:li:dataJob:(urn:li:dataFlow:(airflow,dag_abc,PROD),task_123)"))
              .addInputDatasetField(
                  UrnUtils.getUrn(
                      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD),user_id)"))
              .addOutputDatasetField(
                  UrnUtils.getUrn(
                      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD),user_id)"))
              .build();

      Future<MetadataWriteResponse> response = emitter.emit(dataJobIOPatch);

      System.out.println(response.get().getResponseContent());
    } catch (Exception e) {
      log.error("Failed to emit metadata to DataHub", e);
      throw new RuntimeException(e);
    }
  }
}
