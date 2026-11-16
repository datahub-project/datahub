package io.datahubproject.openapi.v2.models;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.util.Map;
import org.testng.annotations.Test;

public class GenericEntityV2Test {

  private static final String ACTOR = "urn:li:corpuser:datahub";

  @Test
  public void testSystemMetadataWithAuditStampsIsSerializedAsJson() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    SystemMetadata systemMetadata =
        new SystemMetadata()
            .setRunId("run-1")
            .setLastObserved(1L)
            .setAspectCreated(new AuditStamp().setTime(2L).setActor(UrnUtils.getUrn(ACTOR)));

    GenericEntityV2 entity =
        GenericEntityV2.builder()
            .urn(ACTOR)
            .build(
                objectMapper,
                Map.of(
                    Constants.STATUS_ASPECT_NAME,
                    Pair.of((RecordTemplate) new Status().setRemoved(false), systemMetadata)));

    JsonNode aspect =
        objectMapper
            .readTree(objectMapper.writeValueAsString(entity))
            .get("aspects")
            .get(Constants.STATUS_ASPECT_NAME);
    assertEquals(aspect.get("value").get("removed").asBoolean(), false);
    assertEquals(aspect.get("systemMetadata").get("runId").asText(), "run-1");
    assertEquals(aspect.get("systemMetadata").get("aspectCreated").get("actor").asText(), ACTOR);
  }
}
