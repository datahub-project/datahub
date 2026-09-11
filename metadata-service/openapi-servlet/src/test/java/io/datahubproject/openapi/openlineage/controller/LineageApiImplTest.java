package io.datahubproject.openapi.openlineage.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

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
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
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
