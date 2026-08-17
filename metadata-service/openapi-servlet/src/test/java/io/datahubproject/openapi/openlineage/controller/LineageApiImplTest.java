package io.datahubproject.openapi.openlineage.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.openlineage.exception.InvalidOpenLineageEventException;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openapi.openlineage.validation.JsonSchemaOpenLineageRequestValidator;
import io.datahubproject.openapi.openlineage.validation.OpenLineageSchemaCatalog;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.openlineage.client.OpenLineage;
import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LineageApiImplTest {
  private LineageApiImpl controller;
  private EntityServiceImpl entityService;
  private RunEventMapper runEventMapper;

  @BeforeMethod
  public void setup() {
    runEventMapper = spy(new RunEventMapper());
    controller =
        new LineageApiImpl(
            new JsonSchemaOpenLineageRequestValidator(new OpenLineageSchemaCatalog()),
            new OpenLineageEventDeserializer(),
            runEventMapper);
    entityService = mock(EntityServiceImpl.class);

    DatahubOpenlineageConfig config =
        DatahubOpenlineageConfig.builder()
            .fabricType(FabricType.PROD)
            .materializeDataset(true)
            .includeSchemaMetadata(true)
            .build();

    ReflectionTestUtils.setField(
        controller,
        "_mappingConfig",
        RunEventMapper.MappingConfig.builder().datahubConfig(config).build());
    ReflectionTestUtils.setField(controller, "_entityService", entityService);
    ReflectionTestUtils.setField(controller, "_authorizerChain", mock(AuthorizerChain.class));
    ReflectionTestUtils.setField(
        controller,
        "systemOperationContext",
        TestOperationContexts.systemContextNoSearchAuthorization());
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    ReflectionTestUtils.setField(controller, "request", request);

    Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "testuser"));
    AuthenticationContext.setAuthentication(authentication);
  }

  @Test
  public void testRunEventDispatchEmitsDataProcessInstanceAspects() {
    ResponseEntity<Void> response = post(validRunEventJson());

    assertEquals(response.getStatusCode(), HttpStatus.ACCEPTED);

    List<MetadataChangeProposal> proposals = ingestedProposals();
    assertTrue(hasAspect(proposals, "dataProcessInstanceRunEvent"));
    assertTrue(hasAspect(proposals, "dataProcessInstanceProperties"));
  }

  @Test
  public void testJobEventDispatchDoesNotRequireRunBlock() {
    ResponseEntity<Void> response =
        post(
            "{"
                + "\"eventTime\":\"2026-04-14T10:00:00Z\","
                + "\"producer\":\"https://example.com/my-pipeline-tool\","
                + "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent\","
                + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
                + "\"inputs\":[],"
                + "\"outputs\":[]"
                + "}");

    assertEquals(response.getStatusCode(), HttpStatus.ACCEPTED);

    List<MetadataChangeProposal> proposals = ingestedProposals();
    assertTrue(hasAspect(proposals, "dataJobInfo"));
    assertFalse(
        proposals.stream()
            .anyMatch(proposal -> "dataProcessInstance".equals(proposal.getEntityType())));
  }

  @Test
  public void testOnlyFullyEquivalentProposalsAreRemoved() {
    SystemMetadata systemMetadata =
        com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata("test-run");
    MetadataChangeProposal first =
        proposal("{\"removed\":false}").setSystemMetadata(systemMetadata);
    MetadataChangeProposal exactDuplicate =
        proposal("{\"removed\":false}").setSystemMetadata(systemMetadata);
    MetadataChangeProposal differentPayload =
        proposal("{\"removed\":true}").setSystemMetadata(systemMetadata);
    doReturn(Stream.of(first, exactDuplicate, differentPayload))
        .when(runEventMapper)
        .map(any(OpenLineage.JobEvent.class), any(RunEventMapper.MappingConfig.class));

    ResponseEntity<Void> response = post(validJobEventJson());

    assertEquals(response.getStatusCode(), HttpStatus.ACCEPTED);
    assertEquals(ingestedProposals(), List.of(first, differentPayload));
  }

  @Test
  public void testNaiveTimestampsUseSystemDefaultZone() throws Exception {
    TimeZone originalTimeZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      String eventJson =
          validRunEventJson()
              .replace("2026-04-14T10:00:00Z", "2026-04-14T10:00:00")
              .replace(
                  "\"run\":{\"runId\":\"d46e465b-d358-4d32-83d4-df660ff614dd\"}",
                  "\"run\":{\"runId\":\"d46e465b-d358-4d32-83d4-df660ff614dd\","
                      + "\"facets\":{\"nominalTime\":{"
                      + "\"nominalStartTime\":\"2026-04-14T11:00:00\"}}}}");

      OpenLineage.RunEvent event =
          new OpenLineageEventDeserializer()
              .deserialize(new ObjectMapper().readTree(eventJson), OpenLineage.RunEvent.class);

      assertEquals(event.getEventTime().toInstant(), Instant.parse("2026-04-14T17:00:00Z"));
      assertEquals(
          event.getRun().getFacets().getNominalTime().getNominalStartTime().toInstant(),
          Instant.parse("2026-04-14T18:00:00Z"));
    } finally {
      TimeZone.setDefault(originalTimeZone);
    }
  }

  @Test
  public void testLegacyNonUuidRunIdUsesStableCompatibleIdentity() throws Exception {
    String eventJson =
        validRunEventJson()
            .replace("\"namespace\":\"crm\"", "\"namespace\":\"job_namespace\"")
            .replace("\"name\":\"load.customer\"", "\"name\":\"job_name\"")
            .replace("d46e465b-d358-4d32-83d4-df660ff614dd", "run_id");

    OpenLineage.RunEvent event =
        new OpenLineageEventDeserializer()
            .deserialize(new ObjectMapper().readTree(eventJson), OpenLineage.RunEvent.class);

    assertEquals(event.getRun().getRunId().toString(), "36836b81-b861-3ffa-ae86-caa51d5578a5");
  }

  @Test
  public void testInvalidEventTimeRaisesInvalidOpenLineageEvent() {
    InvalidOpenLineageEventException exception =
        expectThrows(
            InvalidOpenLineageEventException.class,
            () -> post(validJobEventJson("not-a-timestamp")));

    assertEquals(exception.getValidationErrors().size(), 1);
    assertEquals(exception.getValidationErrors().get(0).path(), "$");
    assertEquals(exception.getValidationErrors().get(0).rule(), "deserialization");
  }

  @Test
  public void testMissingLegacySchemaUrlIsAccepted() {
    ResponseEntity<Void> response =
        post(
            "{"
                + "\"eventTime\":\"2026-04-14T10:00:00Z\","
                + "\"producer\":\"https://example.com/my-pipeline-tool\","
                + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
                + "\"inputs\":[],"
                + "\"outputs\":[]"
                + "}");

    assertEquals(response.getStatusCode(), HttpStatus.ACCEPTED);
    verify(entityService, times(1)).ingestProposal(any(), any(AspectsBatch.class), eq(true));
  }

  @Test
  public void testDatasetEventDispatchMaterializesDatasetOnly() {
    ResponseEntity<Void> response =
        post(
            "{"
                + "\"eventTime\":\"2026-04-14T10:00:00Z\","
                + "\"producer\":\"https://example.com/my-pipeline-tool\","
                + "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent\","
                + "\"dataset\":{\"namespace\":\"snowflake\",\"name\":\"db.schema.table\"}"
                + "}");

    assertEquals(response.getStatusCode(), HttpStatus.ACCEPTED);

    List<MetadataChangeProposal> proposals = new ArrayList<>(ingestedProposals());
    assertTrue(hasAspect(proposals, "datasetKey"));
    assertTrue(proposals.stream().allMatch(proposal -> "dataset".equals(proposal.getEntityType())));
  }

  private List<MetadataChangeProposal> ingestedProposals() {
    ArgumentCaptor<AspectsBatch> batchCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    verify(entityService, times(1))
        .ingestProposal(any(OperationContext.class), batchCaptor.capture(), eq(true));
    return batchCaptor.getValue().getMCPItems().stream()
        .map(MCPItem::getMetadataChangeProposal)
        .toList();
  }

  private ResponseEntity<Void> post(String body) {
    return controller.postRunEventRaw(body.getBytes(StandardCharsets.UTF_8));
  }

  private static String validRunEventJson() {
    return "{"
        + "\"eventType\":\"COMPLETE\","
        + "\"eventTime\":\"2026-04-14T10:00:00Z\","
        + "\"producer\":\"https://example.com/my-pipeline-tool\","
        + "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent\","
        + "\"run\":{\"runId\":\"d46e465b-d358-4d32-83d4-df660ff614dd\"},"
        + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
        + "\"inputs\":[],"
        + "\"outputs\":[]"
        + "}";
  }

  private static String validJobEventJson() {
    return validJobEventJson("2026-04-14T10:00:00Z");
  }

  private static String validJobEventJson(String eventTime) {
    return "{"
        + "\"eventTime\":\""
        + eventTime
        + "\","
        + "\"producer\":\"https://example.com/my-pipeline-tool\","
        + "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent\","
        + "\"job\":{\"namespace\":\"crm\",\"name\":\"load.customer\"},"
        + "\"inputs\":[],"
        + "\"outputs\":[]"
        + "}";
  }

  private static MetadataChangeProposal proposal(String value) {
    return new MetadataChangeProposal()
        .setEntityUrn(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)"))
        .setEntityType("dataset")
        .setAspectName("status")
        .setChangeType(ChangeType.UPSERT)
        .setAspect(
            new GenericAspect()
                .setContentType("application/json")
                .setValue(ByteString.copyString(value, StandardCharsets.UTF_8)));
  }

  private static boolean hasAspect(List<MetadataChangeProposal> proposals, String aspectName) {
    return proposals.stream().anyMatch(proposal -> aspectName.equals(proposal.getAspectName()));
  }
}
