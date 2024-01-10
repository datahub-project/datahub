package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.builder.DatasetPropertiesPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DatasetCustomPropertiesAdd {

  private DatasetCustomPropertiesAdd() {}

  /**
   * Adds properties to an existing custom properties aspect without affecting any existing
   * properties
   *
   * @param args
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    MetadataChangeProposal datasetPropertiesProposal =
        new DatasetPropertiesPatchBuilder()
            .urn(UrnUtils.toDatasetUrn("hive", "fct_users_deleted", "PROD"))
            .addCustomProperty("cluster_name", "datahubproject.acryl.io")
            .addCustomProperty("retention_time", "2 years")
            .build();

    String token = "";
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));
    try {
      Future<MetadataWriteResponse> response = emitter.emit(datasetPropertiesProposal);

      System.out.println(response.get().getResponseContent());
    } catch (Exception e) {
      log.error("Failed to emit metadata to DataHub", e);
      throw e;
    } finally {
      emitter.close();
    }
  }
}
