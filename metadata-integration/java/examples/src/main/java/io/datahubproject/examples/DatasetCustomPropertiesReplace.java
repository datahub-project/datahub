package io.datahubproject.examples;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import datahub.client.MetadataWriteResponse;
import datahub.client.patch.PatchOperationType;
import datahub.client.patch.dataset.DatasetPropertiesPatchBuilder;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


@Slf4j
class DatasetCustomPropertiesReplace {

  private DatasetCustomPropertiesReplace() {

  }

  public static void main(String[] args) throws IOException {
    StringMap properties = new StringMap();
    properties.put("cluster_name", "datahubproject.acryl.io");
    properties.put("retention_time", "2 years");
    MetadataChangeProposalWrapper datasetPropertiesProposal = MetadataChangeProposalWrapper
        .builder()
        .entityType(DATASET_ENTITY_NAME)
        .entityUrn(UrnUtils.toDatasetUrn("hive", "fct_users_deleted", "PROD"))
        .upsert()
        .aspect(new DatasetProperties()
            .setCustomProperties(properties)
            .setDescription("table containing all the users deleted on a single day")
        )
        .aspectName(DATASET_PROPERTIES_ASPECT_NAME)
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


