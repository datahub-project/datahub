package io.datahubproject.openapi.v1.entities;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.util.MappingUtil;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntitiesControllerTest {

  private OperationContext systemOperationContext;

  @Mock private EntityService<ChangeItemImpl> entityService;

  @Mock private ObjectMapper objectMapper;

  @Mock private AuthorizerChain authorizerChain;

  @Mock private MetricUtils metricUtils;

  @Mock private HttpServletRequest request;

  @Mock private Authentication authentication;

  @Mock private EntityRegistry entityRegistry;

  private EntitiesController controller;

  private AutoCloseable mocks;

  @BeforeMethod
  public void setup() {
    mocks = MockitoAnnotations.openMocks(this);

    systemOperationContext =
        TestOperationContexts.Builder.builder()
            .systemTelemetryContextSupplier(
                () -> SystemTelemetryContext.TEST.toBuilder().metricUtils(metricUtils).build())
            .buildSystemContext();

    // Create controller
    controller =
        new EntitiesController(
            systemOperationContext, entityService, objectMapper, authorizerChain);

    // Setup authentication
    Actor actor = new Actor(ActorType.USER, "urn:li:corpuser:testuser");
    when(authentication.getActor()).thenReturn(actor);

    // request
    when(request.getHeader(anyString())).thenReturn("");
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testGetEntitiesSuccess() throws Exception {
    // Given
    String[] urns = {"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"};
    String[] aspectNames = {"datasetProperties", "status"};

    Set<Urn> entityUrns = new HashSet<>();
    entityUrns.add(UrnUtils.getUrn(urns[0]));

    // Mock entity responses
    Map<Urn, EntityResponse> mockResponses = new HashMap<>();
    EntityResponse entityResponse = new EntityResponse();
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    entityResponse.setAspects(aspectMap);
    mockResponses.put(entityUrns.iterator().next(), entityResponse);

    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<OperationContext> opContext = Mockito.mockStatic(OperationContext.class);
        MockedStatic<AuthUtil> authUtil = Mockito.mockStatic(AuthUtil.class);
        MockedStatic<MappingUtil> mappingUtil = Mockito.mockStatic(MappingUtil.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);

      OperationContext mockOpContext = mock(OperationContext.class);
      when(mockOpContext.getEntityAspectNames(anyString()))
          .thenReturn(new HashSet<>(Arrays.asList(aspectNames)));

      opContext
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      authUtil.when(() -> AuthUtil.isAPIAuthorizedEntityUrns(any(), any(), any())).thenReturn(true);

      when(entityService.getEntitiesV2(any(), anyString(), anySet(), anySet()))
          .thenReturn(mockResponses);

      Map<String, Object> mappedResponses = new HashMap<>();
      mappingUtil
          .when(() -> MappingUtil.mapServiceResponse(any(), any()))
          .thenReturn(mappedResponses);

      // When
      ResponseEntity<?> response = controller.getEntities(request, urns, aspectNames);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.OK);
      assertNotNull(response.getBody());
      verify(metricUtils).increment(eq(MetricRegistry.name("getEntities", "success")), eq(1d));
    }
  }

  @Test(expectedExceptions = UnauthorizedException.class)
  public void testGetEntitiesUnauthorized() throws Exception {
    // Given
    String[] urns = {"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"};

    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<OperationContext> opContext = Mockito.mockStatic(OperationContext.class);
        MockedStatic<AuthUtil> authUtil = Mockito.mockStatic(AuthUtil.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);

      OperationContext mockOpContext = mock(OperationContext.class);
      opContext
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      authUtil
          .when(() -> AuthUtil.isAPIAuthorizedEntityUrns(any(), any(), any()))
          .thenReturn(false);

      // When/Then
      controller.getEntities(request, urns, null);
    }
  }

  @Test
  public void testGetEntitiesEmptyUrns() throws Exception {
    // Given
    String[] urns = {};

    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<OperationContext> opContext = Mockito.mockStatic(OperationContext.class);
        MockedStatic<AuthUtil> authUtil = Mockito.mockStatic(AuthUtil.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);

      OperationContext mockOpContext = mock(OperationContext.class);
      opContext
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      authUtil.when(() -> AuthUtil.isAPIAuthorizedEntityUrns(any(), any(), any())).thenReturn(true);

      // When
      ResponseEntity<?> response = controller.getEntities(request, urns, null);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.OK);
      assertNotNull(response.getBody());
      verify(entityService, never()).getEntitiesV2(any(), anyString(), anySet(), anySet());
    }
  }

  @Test
  public void testDeleteEntitiesHardDelete() throws Exception {
    // Given
    String[] urns = {"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"};
    boolean soft = false;

    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<OperationContext> opContext = Mockito.mockStatic(OperationContext.class);
        MockedStatic<AuthUtil> authUtil = Mockito.mockStatic(AuthUtil.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);

      OperationContext mockOpContext = mock(OperationContext.class);
      opContext
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      authUtil.when(() -> AuthUtil.isAPIAuthorizedEntityUrns(any(), any(), any())).thenReturn(true);

      // Mock hard delete
      RollbackRunResult rollbackResult = mock(RollbackRunResult.class);
      when(entityService.deleteUrn(any(), any())).thenReturn(rollbackResult);

      // When
      ResponseEntity<?> response = controller.deleteEntities(request, urns, soft, false);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.OK);
      assertNotNull(response.getBody());
      verify(entityService, times(1)).deleteUrn(any(), any());
      verify(metricUtils).increment(eq(MetricRegistry.name("getEntities", "success")), eq(1d));
    }
  }

  @Test
  public void testGetEntitiesWithException() throws Exception {
    // Given
    String[] urns = {"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"};

    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<OperationContext> opContext = Mockito.mockStatic(OperationContext.class);
        MockedStatic<AuthUtil> authUtil = Mockito.mockStatic(AuthUtil.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);

      OperationContext mockOpContext = mock(OperationContext.class);
      when(mockOpContext.getEntityAspectNames(anyString())).thenReturn(new HashSet<>());

      opContext
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      authUtil.when(() -> AuthUtil.isAPIAuthorizedEntityUrns(any(), any(), any())).thenReturn(true);

      when(entityService.getEntitiesV2(any(), anyString(), anySet(), anySet()))
          .thenThrow(new RuntimeException("Test exception"));

      // When/Then
      try {
        controller.getEntities(request, urns, null);
      } catch (RuntimeException e) {
        assertTrue(e.getMessage().contains("Failed to batch get entities"));
        verify(metricUtils).increment(eq(MetricRegistry.name("getEntities", "failed")), eq(1d));
      }
    }
  }
}
