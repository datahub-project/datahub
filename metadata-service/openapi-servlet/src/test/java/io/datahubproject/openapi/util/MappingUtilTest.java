package io.datahubproject.openapi.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.generated.ChangeType;
import io.datahubproject.openapi.generated.EnvelopedAspect;
import io.datahubproject.openapi.generated.MetadataChangeProposal;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.testng.annotations.Test;

public class MappingUtilTest {

  private static final String ACTOR = "urn:li:corpuser:datahub";

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testMapToServiceProposalKeepsHeaders() {
    MetadataChangeProposal proposal =
        MetadataChangeProposal.builder()
            .entityType(Constants.DATASET_ENTITY_NAME)
            .entityUrn("urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)")
            .changeType(ChangeType.UPSERT)
            .aspectName(Constants.STATUS_ASPECT_NAME)
            .headers(Map.of(Constants.IF_ENTITY_EXISTS_HEADER_NAME, "true"))
            .build();

    assertEquals(
        MappingUtil.mapToServiceProposal(proposal, objectMapper).getHeaders(),
        new StringMap(Map.of(Constants.IF_ENTITY_EXISTS_HEADER_NAME, "true")));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIngestProposalReportsDroppedWriteAsNotCommitted() {
    EntityService<ChangeItemImpl> entityService = mock(EntityService.class);
    when(entityService.ingestProposal(any(), any(), anyBoolean())).thenReturn(List.of());
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)");
    com.linkedin.mxe.MetadataChangeProposal proposal =
        new com.linkedin.mxe.MetadataChangeProposal()
            .setEntityType(Constants.DATASET_ENTITY_NAME)
            .setEntityUrn(urn)
            .setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT)
            .setAspectName(Constants.STATUS_ASPECT_NAME)
            .setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)))
            .setHeaders(new StringMap(Map.of(Constants.IF_ENTITY_EXISTS_HEADER_NAME, "true")));

    Pair<String, Boolean> result =
        MappingUtil.ingestProposal(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            proposal,
            ACTOR,
            entityService,
            false);

    assertEquals(result, Pair.of(urn.toString(), false));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIngestProposalRejectsUnsupportedChangeType() {
    EntityService<ChangeItemImpl> entityService = mock(EntityService.class);
    com.linkedin.mxe.MetadataChangeProposal proposal =
        new com.linkedin.mxe.MetadataChangeProposal()
            .setEntityType(Constants.DATASET_ENTITY_NAME)
            .setEntityUrn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,table,PROD)"))
            .setChangeType(com.linkedin.events.metadata.ChangeType.DELETE)
            .setAspectName(Constants.STATUS_ASPECT_NAME);

    HttpClientErrorException exception =
        expectThrows(
            HttpClientErrorException.class,
            () ->
                MappingUtil.ingestProposal(
                    TestOperationContexts.systemContextNoSearchAuthorization(),
                    proposal,
                    ACTOR,
                    entityService,
                    false));

    assertEquals(exception.getStatusCode(), HttpStatus.BAD_REQUEST);
    verify(entityService, never()).ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
  }

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
