package io.datahubproject.openapi.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.openapi.generated.EnvelopedAspect;
import org.testng.annotations.Test;

public class MappingUtilTest {

  private static final String ACTOR = "urn:li:corpuser:datahub";

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testMapEnvelopedAspectMapsSystemMetadata() throws Exception {
    JsonNode systemMetadata =
        mapSystemMetadata(new SystemMetadata().setRunId("run-1").setLastObserved(1L));

    assertEquals(systemMetadata.get("runId").asText(), "run-1");
    assertEquals(systemMetadata.get("lastObserved").asLong(), 1L);
  }

  @Test
  public void testMapEnvelopedAspectMapsSystemMetadataWithAuditStamps() throws Exception {
    JsonNode systemMetadata =
        mapSystemMetadata(
            new SystemMetadata()
                .setRunId("run-1")
                .setLastObserved(1L)
                .setAspectCreated(new AuditStamp().setTime(2L).setActor(UrnUtils.getUrn(ACTOR))));

    assertEquals(systemMetadata.get("aspectCreated").get("actor").asText(), ACTOR);
    assertEquals(systemMetadata.get("aspectCreated").get("time").asLong(), 2L);
  }

  private JsonNode mapSystemMetadata(SystemMetadata systemMetadata) throws Exception {
    com.linkedin.entity.EnvelopedAspect envelopedAspect =
        new com.linkedin.entity.EnvelopedAspect()
            .setName(Constants.STATUS_ASPECT_NAME)
            .setType(AspectType.VERSIONED)
            .setVersion(0L)
            .setCreated(new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn(ACTOR)))
            .setValue(new Aspect(new Status().setRemoved(false).data()))
            .setSystemMetadata(systemMetadata);

    EnvelopedAspect mapped = MappingUtil.mapEnvelopedAspect(envelopedAspect, objectMapper);

    assertNotNull(mapped.getSystemMetadata());
    return objectMapper.readTree(objectMapper.writeValueAsString(mapped)).get("systemMetadata");
  }
}
