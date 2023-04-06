package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import datahub.client.MetadataWriteResponse;
import datahub.client.patch.PatchOperationType;
import datahub.client.patch.dataset.DatasetPropertiesPatchBuilder;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class DatasetCustomProperties {

  private DatasetCustomProperties() {

  }

    public static void main(String[] args) throws IOException {
      MetadataChangeProposal datasetPropertiesProposal = new DatasetPropertiesPatchBuilder()
          .urn(UrnUtils.toDatasetUrn("hive", "fct_users_deleted", "PROD"))
          .op(PatchOperationType.ADD)
          .customPropertiesPatchBuilder()
          .addProperty("cluster_name", "datahubproject.acryl.io")
          .addProperty("retention_time", "2 years")
          .getParent()
          .build();

      String token = "";
      RestEmitter emitter = RestEmitter.createWithDefaults();
      /**(
          b -> b.server("http://localhost:8080")
              .token(token)
      );**/
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


