package io.datahubproject.openapi.openlineage.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.FabricType;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.exception.UnprocessableEntityException;
import io.datahubproject.openapi.openlineage.config.DatahubOpenlineageProperties;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.model.LineageBatchResult;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Behavioural tests for the OpenLineage REST endpoint: authorization is enforced, proposals are
 * ingested as one batch, and the three failure modes map to distinct status codes.
 */
public class LineageApiImplTest {

  /** A minimal well-formed OpenLineage START event with one input dataset. */
  private static final String SAMPLE_START_EVENT =
      "{"
          + "\"eventType\":\"START\","
          + "\"eventTime\":\"2024-01-01T10:00:00.000Z\","
          + "\"run\":{\"runId\":\"d46e465b-d358-4d32-83d4-df660ff614dd\"},"
          + "\"job\":{\"namespace\":\"my_namespace\",\"name\":\"my_job\"},"
          + "\"inputs\":[{\"namespace\":\"postgres://my-host:5432\",\"name\":\"my_db.my_schema.events\"}],"
          + "\"producer\":\"https://github.com/apache/airflow/tree/providers-openlineage/1.0.0\""
          + "}";

  /** A JobEvent: job metadata and dataset edges with no run attached. */
  private static final String SAMPLE_JOB_EVENT =
      "{"
          + "\"eventTime\":\"2024-01-01T10:00:00.000Z\","
          + "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent\","
          + "\"job\":{\"namespace\":\"my_namespace\",\"name\":\"my_job\"},"
          + "\"inputs\":[{\"namespace\":\"postgres://my-host:5432\",\"name\":\"my_db.my_schema.source\"}],"
          + "\"producer\":\"https://github.com/apache/airflow/tree/1.0.0\""
          + "}";

  /** A DatasetEvent: dataset metadata with neither a run nor a job. */
  private static final String SAMPLE_DATASET_EVENT =
      "{"
          + "\"eventTime\":\"2024-01-01T10:00:00.000Z\","
          + "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent\","
          + "\"dataset\":{\"namespace\":\"postgres://my-host:5432\",\"name\":\"my_db.my_schema.events\"},"
          + "\"producer\":\"https://github.com/apache/airflow/tree/1.0.0\""
          + "}";

  /** Same JobEvent with no schemaURL, so classification has to fall back to the event's shape. */
  private static final String JOB_EVENT_WITHOUT_SCHEMA_URL =
      SAMPLE_JOB_EVENT.replace(
          "\"schemaURL\":\"https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/JobEvent\",",
          "");

  /**
   * Deserializes cleanly but cannot be converted: an empty job name yields an empty DataFlow key,
   * which URN construction rejects.
   */
  private static final String EVENT_WITH_EMPTY_JOB_NAME =
      SAMPLE_START_EVENT.replace("\"name\":\"my_job\"", "\"name\":\"\"");

  @Mock private EntityServiceImpl entityService;
  @Mock private AuthorizerChain authorizerChain;
  @Mock private HttpServletRequest request;
  @Mock private Authentication authentication;

  private LineageApiImpl controller;
  private DatahubOpenlineageProperties properties;
  private AutoCloseable mocks;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);

    OperationContext systemOperationContext =
        TestOperationContexts.Builder.builder().buildSystemContext();

    RunEventMapper.MappingConfig mappingConfig =
        RunEventMapper.MappingConfig.builder()
            .datahubConfig(
                DatahubOpenlineageConfig.builder()
                    .fabricType(FabricType.PROD)
                    .materializeDataset(true)
                    .build())
            .build();

    controller = new LineageApiImpl();
    ReflectionTestUtils.setField(controller, "systemOperationContext", systemOperationContext);
    ReflectionTestUtils.setField(controller, "_mappingConfig", mappingConfig);
    ReflectionTestUtils.setField(controller, "_entityService", entityService);
    ReflectionTestUtils.setField(controller, "_authorizerChain", authorizerChain);
    ReflectionTestUtils.setField(controller, "request", request);
    properties = new DatahubOpenlineageProperties();
    ReflectionTestUtils.setField(controller, "_properties", properties);

    when(authentication.getActor())
        .thenReturn(new Actor(ActorType.USER, "urn:li:corpuser:testuser"));
    when(request.getHeader(anyString())).thenReturn("");
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testAuthorizedActorIngestsOneBatch() {
    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<EntityAuthorizationUtils> authUtil =
            Mockito.mockStatic(EntityAuthorizationUtils.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);
      authUtil
          .when(() -> EntityAuthorizationUtils.isAPIAuthorizedIngest(any(), any(), any()))
          .thenAnswer(inv -> allow(inv.getArgument(2)));

      ResponseEntity<Void> response = controller.postRunEventRaw(SAMPLE_START_EVENT);

      assertEquals(response.getStatusCode(), HttpStatus.CREATED);
      // One batch call rather than one call per proposal.
      verify(entityService, times(1)).ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
    }
  }

  @Test(expectedExceptions = UnauthorizedException.class)
  public void testUnauthorizedActorIsRejectedBeforeIngest() {
    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<EntityAuthorizationUtils> authUtil =
            Mockito.mockStatic(EntityAuthorizationUtils.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);
      authUtil
          .when(() -> EntityAuthorizationUtils.isAPIAuthorizedIngest(any(), any(), any()))
          .thenAnswer(inv -> deny(inv.getArgument(2)));

      try {
        controller.postRunEventRaw(SAMPLE_START_EVENT);
      } finally {
        verify(entityService, never()).ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
      }
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMalformedPayloadIsRejected() {
    controller.postRunEventRaw("{ not json");
  }

  @Test(expectedExceptions = UnprocessableEntityException.class)
  public void testUnconvertibleEventIsRejected() {
    try (MockedStatic<AuthenticationContext> authContext =
        Mockito.mockStatic(AuthenticationContext.class)) {
      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);
      controller.postRunEventRaw(EVENT_WITH_EMPTY_JOB_NAME);
    }
  }

  @Test
  public void testJobEventIsDispatchedAndIngested() {
    assertDispatched(SAMPLE_JOB_EVENT);
  }

  @Test
  public void testDatasetEventIsDispatchedAndIngested() {
    assertDispatched(SAMPLE_DATASET_EVENT);
  }

  /**
   * A client that serializes optional fields emits "run": null on a JobEvent. JsonNode.has() is
   * true for an explicit null, so shape classification used to send it down the RunEvent path and
   * reproduce the very null-run failure this dispatch exists to prevent.
   */
  @Test
  public void testExplicitNullRunIsNotClassifiedAsARunEvent() {
    assertDispatched("{\"run\":null," + JOB_EVENT_WITHOUT_SCHEMA_URL.substring(1));
  }

  @Test
  public void testEventWithoutSchemaUrlIsClassifiedByShape() {
    // A JobEvent with no schemaURL has a job but no run; before dispatch this deserialized as a
    // RunEvent with a null run and produced a 500.
    assertDispatched(JOB_EVENT_WITHOUT_SCHEMA_URL);
  }

  private void assertDispatched(String body) {
    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<EntityAuthorizationUtils> authUtil =
            Mockito.mockStatic(EntityAuthorizationUtils.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);
      authUtil
          .when(() -> EntityAuthorizationUtils.isAPIAuthorizedIngest(any(), any(), any()))
          .thenAnswer(inv -> allow(inv.getArgument(2)));

      ResponseEntity<Void> response = controller.postRunEventRaw(body);

      assertEquals(response.getStatusCode(), HttpStatus.CREATED);
      verify(entityService, times(1)).ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
    }
  }

  @Test
  public void testBatchIngestsEveryEventAsOneWrite() {
    LineageBatchResult result =
        postBatch(batch(SAMPLE_START_EVENT, SAMPLE_JOB_EVENT, SAMPLE_DATASET_EVENT));

    assertEquals(result.getStatus(), LineageBatchResult.STATUS_SUCCESS);
    assertEquals(result.getSummary().getReceived(), 3);
    assertEquals(result.getSummary().getSuccessful(), 3);
    assertTrue(result.getFailedEvents().isEmpty());
    // The point of a batch: three events, one write.
    verify(entityService, times(1)).ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
  }

  @Test
  public void testBatchReportsAnUnconvertibleEventAndIngestsTheRest() {
    LineageBatchResult result =
        postBatch(batch(SAMPLE_START_EVENT, EVENT_WITH_EMPTY_JOB_NAME, SAMPLE_JOB_EVENT));

    assertEquals(result.getStatus(), LineageBatchResult.STATUS_PARTIAL_SUCCESS);
    assertEquals(result.getSummary().getReceived(), 3);
    assertEquals(result.getSummary().getSuccessful(), 2);
    assertEquals(result.getSummary().getFailed(), 1);
    assertEquals(result.getFailedEvents().size(), 1);
    // The index is the producer's only handle on which event it has to fix.
    assertEquals(result.getFailedEvents().get(0).getIndex(), 1);
    // Conversion is deterministic, so resending the same bytes cannot help.
    assertEquals(result.getFailedEvents().get(0).isRetriable(), false);
    assertEquals(result.getSummary().getNonRetriable(), 1);
    verify(entityService, times(1)).ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
  }

  @Test
  public void testEmptyBatchIngestsNothing() {
    LineageBatchResult result = postBatch("[]");

    assertEquals(result.getStatus(), LineageBatchResult.STATUS_SUCCESS);
    assertEquals(result.getSummary().getReceived(), 0);
    // A producer flushing an empty buffer is not an error, but it must not open a transaction.
    verify(entityService, never()).ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBatchRejectsABodyThatIsNotAnArray() {
    controller.postEventBatchRaw(SAMPLE_START_EVENT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBatchRejectsMoreEventsThanConfigured() {
    properties.setMaxBatchSize(1);
    controller.postEventBatchRaw(batch(SAMPLE_START_EVENT, SAMPLE_JOB_EVENT));
  }

  @Test(expectedExceptions = UnauthorizedException.class)
  public void testBatchIsRefusedWholeWhenTheActorIsUnauthorized() {
    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<EntityAuthorizationUtils> authUtil =
            Mockito.mockStatic(EntityAuthorizationUtils.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);
      authUtil
          .when(() -> EntityAuthorizationUtils.isAPIAuthorizedIngest(any(), any(), any()))
          .thenAnswer(inv -> deny(inv.getArgument(2)));

      try {
        // Authorization is a property of the actor, not of an individual event, so a batch is
        // refused whole rather than reported as a per-event failure.
        controller.postEventBatchRaw(batch(SAMPLE_START_EVENT, SAMPLE_JOB_EVENT));
      } finally {
        verify(entityService, never()).ingestProposal(any(), any(AspectsBatch.class), anyBoolean());
      }
    }
  }

  /**
   * The response is the spec's wire contract, and Lombok's getter names are camelCase, so the
   * snake_case field names have to survive serialization.
   */
  @Test
  public void testResultSerializesWithTheSpecFieldNames() throws Exception {
    String json =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .writeValueAsString(
                LineageBatchResult.of(
                    2, List.of(new LineageBatchResult.FailedEvent(1, "nope", false))));

    assertTrue(json.contains("\"failed_events\""), json);
    assertTrue(json.contains("\"non_retriable\""), json);
  }

  private LineageBatchResult postBatch(String body) {
    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<EntityAuthorizationUtils> authUtil =
            Mockito.mockStatic(EntityAuthorizationUtils.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);
      authUtil
          .when(() -> EntityAuthorizationUtils.isAPIAuthorizedIngest(any(), any(), any()))
          .thenAnswer(inv -> allow(inv.getArgument(2)));

      ResponseEntity<LineageBatchResult> response = controller.postEventBatchRaw(body);
      assertEquals(response.getStatusCode(), HttpStatus.OK);
      return response.getBody();
    }
  }

  private static String batch(String... events) {
    return "[" + String.join(",", events) + "]";
  }

  private static List<Pair<MetadataChangeProposal, Integer>> allow(
      java.util.Collection<MetadataChangeProposal> mcps) {
    return mcps.stream()
        .map(mcp -> Pair.of(mcp, com.linkedin.restli.common.HttpStatus.S_200_OK.getCode()))
        .collect(Collectors.toList());
  }

  private static List<Pair<MetadataChangeProposal, Integer>> deny(
      java.util.Collection<MetadataChangeProposal> mcps) {
    return mcps.stream()
        .map(mcp -> Pair.of(mcp, com.linkedin.restli.common.HttpStatus.S_403_FORBIDDEN.getCode()))
        .collect(Collectors.toList());
  }
}
