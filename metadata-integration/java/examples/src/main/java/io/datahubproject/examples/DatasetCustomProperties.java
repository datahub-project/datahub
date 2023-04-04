package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import datahub.client.patch.PatchOperationType;
import datahub.client.patch.dataset.DatasetPropertiesPatchBuilder;
import datahub.client.rest.RestEmitter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.mxe.MetadataChangeProposal;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class DatasetCustomProperties {

    public static void main(String[] args) throws IOException {
        
    Map<String, String> customProperties = new HashMap<>();
      customProperties.put("cluster_name", "datahub.acryl.io");
      customProperties.put("retention_time", "2 years");
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
        emitter.emit(datasetPropertiesProposal);
      }
      catch (Exception e) {
        log.error("Failed to emit metadata to DataHub", e);
      }
      finally {
        emitter.close();
      }

    }

}


