package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.MetadataWriteResponse;
import datahub.client.patch.dataset.DatasetPropertiesPatchBuilder;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DatasetCustomPropertiesReplace {

  private DatasetCustomPropertiesReplace() {}

  /**
   * Replaces the existing custom properties map with a new map. Fields like dataset name,
   * description etc remain unchanged.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Map<String, String> customPropsMap = new HashMap<>();
    customPropsMap.put("cluster_name", "datahubproject.acryl.io");
    customPropsMap.put("retention_time", "2 years");
    MetadataChangeProposal datasetPropertiesProposal =
        new DatasetPropertiesPatchBuilder()
            .urn(UrnUtils.toDatasetUrn("hive", "fct_users_deleted", "PROD"))
            .setCustomProperties(customPropsMap)
            .build();

    String token = "";
    RestEmitter emitter = RestEmitter.create(b -> b.server("http://localhost:8080").token(token));

    try {
      Future<MetadataWriteResponse> response = emitter.emit(datasetPropertiesProposal);
      System.out.println(response.get().getResponseContent());
    } catch (Exception e) {
      log.error("Failed to emit metadata to DataHub", e);
    } finally {
      emitter.close();
    }
  }
}
