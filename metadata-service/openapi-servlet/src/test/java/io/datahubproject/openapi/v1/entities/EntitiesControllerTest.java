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
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySource;
import com.linkedin.query.QueryStatement;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;
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

  private static final Urn QUERY_URN = UrnUtils.getUrn("urn:li:query:view-entity-queries-test");
  private static final Urn SUBJECT_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,query-subject,PROD)");
  private static final Urn ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:testuser");

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

  /**
   * Documents currently-unenforced behavior for the OpenAPI v2 aspect read path (the v2 {@code GET
   * /openapi/v2/entity/query/&lt;urn&gt;/queryProperties} endpoint delegates to this controller's
   * {@code getEntities}): reading a query entity's queryProperties aspect must require the {@code
   * VIEW_ENTITY_QUERIES} privilege on the query's subject dataset. Expected to fail (the SQL
   * statement is returned instead of an UnauthorizedException) until enforcement is wired in.
   */
  @Test
  public void testGetQueryEntityDeniedWithoutViewEntityQueries() throws Exception {
    String[] urns = {QUERY_URN.toString()};
    String[] aspectNames = {"queryProperties"};

    EntitiesController queryController = queryAuthController(false);
    when(entityService.getEntitiesV2(any(), anyString(), anySet(), anySet()))
        .thenReturn(queryEntityServiceResponse());

    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<AuthUtil> authUtil =
            Mockito.mockStatic(AuthUtil.class, Mockito.CALLS_REAL_METHODS);
        MockedStatic<MappingUtil> mappingUtil = Mockito.mockStatic(MappingUtil.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);
      authUtil.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
      mappingUtil
          .when(() -> MappingUtil.mapServiceResponse(any(), any()))
          .thenReturn(new HashMap<>());

      assertThrows(
          UnauthorizedException.class,
          () -> queryController.getEntities(request, urns, aspectNames));
      verify(entityService, never()).getEntitiesV2(any(), anyString(), anySet(), anySet());
    }
  }

  /** Mirror allow-case: an actor granted VIEW_ENTITY_QUERIES can read the query entity. */
  @Test
  public void testGetQueryEntityAllowedWithViewEntityQueries() throws Exception {
    String[] urns = {QUERY_URN.toString()};
    String[] aspectNames = {"queryProperties"};

    EntitiesController queryController = queryAuthController(true);
    when(entityService.getEntitiesV2(any(), anyString(), anySet(), anySet()))
        .thenReturn(queryEntityServiceResponse());

    try (MockedStatic<AuthenticationContext> authContext =
            Mockito.mockStatic(AuthenticationContext.class);
        MockedStatic<AuthUtil> authUtil =
            Mockito.mockStatic(AuthUtil.class, Mockito.CALLS_REAL_METHODS);
        MockedStatic<MappingUtil> mappingUtil = Mockito.mockStatic(MappingUtil.class)) {

      authContext.when(AuthenticationContext::getAuthentication).thenReturn(authentication);
      authUtil.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
      mappingUtil
          .when(() -> MappingUtil.mapServiceResponse(any(), any()))
          .thenAnswer(
              invocation -> {
                Map<Urn, EntityResponse> responses = invocation.getArgument(0);
                Map<String, Object> mapped = new HashMap<>();
                responses.keySet().forEach(urn -> mapped.put(urn.toString(), new Object()));
                return mapped;
              });

      ResponseEntity<?> response = queryController.getEntities(request, urns, aspectNames);

      assertEquals(response.getStatusCode(), HttpStatus.OK);
      assertNotNull(response.getBody());
      verify(entityService, times(1)).getEntitiesV2(any(), anyString(), anySet(), anySet());
    }
  }

  /**
   * Builds a controller wired with a REAL session/authorization flow (no mocked {@code
   * OperationContext.asSession}, no blanket-mocked {@code AuthUtil}) so privilege-specific
   * enforcement is exercised: the authorizer grants general read access but the query-view
   * privilege group (VIEW_ENTITY_QUERIES / EDIT_ENTITY_QUERIES / EDIT_ENTITY) only when {@code
   * hasQueryViewPrivilege} is true, and the aspect retriever resolves the query's subject dataset.
   */
  private EntitiesController queryAuthController(boolean hasQueryViewPrivilege) {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());
    QuerySubjects querySubjects =
        new QuerySubjects()
            .setSubjects(
                new QuerySubjectArray(
                    java.util.List.of(new QuerySubject().setEntity(SUBJECT_URN))));
    when(aspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(QUERY_URN)), eq(Set.of(Constants.QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                QUERY_URN,
                Map.of(
                    Constants.QUERY_SUBJECTS_ASPECT_NAME,
                    new com.linkedin.entity.Aspect(querySubjects.data()))));

    Set<String> queryViewPrivileges =
        Set.of(
            PoliciesConfig.VIEW_ENTITY_QUERIES_PRIVILEGE.getType(),
            PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType(),
            PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType());
    org.mockito.stubbing.Answer<AuthorizationResult> answer =
        invocation -> {
          AuthorizationRequest authRequest = invocation.getArgument(0);
          boolean allowed =
              hasQueryViewPrivilege || !queryViewPrivileges.contains(authRequest.getPrivilege());
          return new AuthorizationResult(
              authRequest,
              allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
              "view entity queries test");
        };
    when(authorizerChain.authorize(any(AuthorizationRequest.class))).thenAnswer(answer);
    when(authorizerChain.authorize(any(AuthorizationRequest.class), any(Map.class), any()))
        .thenAnswer(answer);

    OperationContext queryAuthSystemContext =
        TestOperationContexts.systemContextNoSearchAuthorization(aspectRetriever);
    return new EntitiesController(
        queryAuthSystemContext, entityService, objectMapper, authorizerChain);
  }

  private Map<Urn, EntityResponse> queryEntityServiceResponse() {
    QueryProperties queryProperties =
        new QueryProperties()
            .setSource(QuerySource.MANUAL)
            .setStatement(
                new QueryStatement()
                    .setLanguage(QueryLanguage.SQL)
                    .setValue("SELECT sensitive FROM restricted_table"))
            .setCreated(new AuditStamp().setActor(ACTOR_URN).setTime(0L))
            .setLastModified(new AuditStamp().setActor(ACTOR_URN).setTime(0L));
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(QUERY_URN);
    entityResponse.setEntityName(Constants.QUERY_ENTITY_NAME);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        Constants.QUERY_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new com.linkedin.entity.Aspect(queryProperties.data())));
    entityResponse.setAspects(aspectMap);
    Map<Urn, EntityResponse> responses = new HashMap<>();
    responses.put(QUERY_URN, entityResponse);
    return responses;
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
  public void testDeleteEntitiesWithExistingContextDoesNotOpenSession() throws Exception {
    Set<Urn> entityUrns =
        Set.of(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"));

    try (MockedStatic<OperationContext> opContext = Mockito.mockStatic(OperationContext.class);
        MockedStatic<AuthUtil> authUtil = Mockito.mockStatic(AuthUtil.class)) {

      OperationContext existingContext = mock(OperationContext.class);
      authUtil.when(() -> AuthUtil.isAPIAuthorizedEntityUrns(any(), any(), any())).thenReturn(true);

      RollbackRunResult rollbackResult = mock(RollbackRunResult.class);
      when(entityService.deleteUrn(any(), any())).thenReturn(rollbackResult);

      ResponseEntity<?> response =
          controller.deleteEntities(
              existingContext, "urn:li:corpuser:testuser", entityUrns, false, false);

      assertEquals(response.getStatusCode(), HttpStatus.OK);
      verify(entityService, times(1)).deleteUrn(eq(existingContext), any());
      opContext.verify(
          () -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()), never());
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
