package com.linkedin.metadata.resources.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.parseq.Task;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityV2ResourceTest {

  private static final Urn QUERY_URN = UrnUtils.getUrn("urn:li:query:auth-test");
  private static final Urn SUBJECT_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,foo,PROD)");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,bar,PROD)");

  private EntityV2Resource entityV2Resource;
  private EntityService<?> entityService;
  private Engine parseqEngine;

  @BeforeMethod
  public void setup() {
    entityV2Resource = new EntityV2Resource();
    entityService = mock(EntityService.class);
    entityV2Resource.setEntityService(entityService);
    entityV2Resource.setAuthorizer(allowAllAuthorizer());
    entityV2Resource.setSystemOperationContext(
        TestOperationContexts.systemContextNoSearchAuthorization());

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    when(mockAuthentication.getActor()).thenReturn(new Actor(ActorType.USER, "user"));

    parseqEngine =
        new EngineBuilder()
            .setTaskExecutor(Runnable::run)
            .setTimerScheduler(Executors.newSingleThreadScheduledExecutor())
            .build();
  }

  @AfterMethod
  public void tearDown() {
    if (parseqEngine != null) {
      parseqEngine.shutdown();
    }
  }

  private <T> T awaitTask(Task<T> task) {
    parseqEngine.blockingRun(task);
    return task.get();
  }

  /**
   * Regression for a crash the previous whole-aspect redaction fix introduced: {@code
   * getEntityV2} returns null for some empty-projection requests (e.g. an explicit {@code
   * aspects=List()}) rather than treating empty as "fetch everything" the way the legacy {@code
   * getEntity} does. {@code Map.of(urn, response)} rejects a null value and throws {@code
   * NullPointerException}, which the surrounding catch block rewrapped into a {@code
   * RuntimeException} that Rest.li surfaced as an HTTP 500 with an internal stack trace. The fix
   * must skip redaction (there is nothing to redact) and return null cleanly instead of crashing.
   */
  @Test
  public void testGetHandlesNullResponseFromEmptyProjectionWithoutThrowing() throws Exception {
    when(entityService.getEntityV2(
            any(OperationContext.class), eq("dataset"), eq(DATASET_URN), any(), eq(true)))
        .thenReturn(null);

    EntityResponse result =
        awaitTask(entityV2Resource.get(DATASET_URN.toString(), new String[0], null));

    assertNull(result);
  }

  @Test
  public void testIsAuthorizedToReadEntities_deniesQueryWhenSubjectNotViewable() throws Exception {
    OperationContext opContext = createUserContextWithViewAuth(denyAllAuthorizer(), querySubjectsRetriever());

    assertFalse(invokeIsAuthorizedToReadEntities(opContext, List.of(QUERY_URN)));
  }

  @Test
  public void testIsAuthorizedToReadEntities_allowsDatasetWithReadPrivilege() throws Exception {
    OperationContext opContext =
        createUserContextWithViewAuth(allowAllAuthorizer(), mock(AspectRetriever.class));

    assertTrue(invokeIsAuthorizedToReadEntities(opContext, List.of(DATASET_URN)));
  }

  @Test
  public void testIsAuthorizedToReadEntities_allowsQueryWhenSubjectViewable() throws Exception {
    OperationContext opContext =
        createUserContextWithViewAuth(allowAllAuthorizer(), querySubjectsRetriever());

    assertTrue(invokeIsAuthorizedToReadEntities(opContext, List.of(QUERY_URN)));
  }

  /**
   * Query READ enforcement now flows through the shared {@code
   * EntityAuthorizationUtils.isAPIAuthorizedEntityUrns} path, whose activation follows the REST API
   * authorization setting — so it is force-enabled here (real tests run with the default-enabled
   * setting; unit test JVMs never initialize it).
   */
  private static boolean invokeIsAuthorizedToReadEntities(
      OperationContext opContext, List<Urn> urns) throws Exception {
    Method method =
        EntityV2Resource.class.getDeclaredMethod(
            "isAuthorizedToReadEntities", OperationContext.class, java.util.Collection.class);
    method.setAccessible(true);
    try (MockedStatic<AuthUtil> authUtil =
        Mockito.mockStatic(AuthUtil.class, Mockito.CALLS_REAL_METHODS)) {
      authUtil.when(AuthUtil::isRestApiAuthorizationEnabled).thenReturn(true);
      return (boolean) method.invoke(null, opContext, urns);
    }
  }

  private static Authorizer denyAllAuthorizer() {
    Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));
    return authorizer;
  }

  private static Authorizer allowAllAuthorizer() {
    Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.ALLOW, ""));
    return authorizer;
  }

  private static AspectRetriever querySubjectsRetriever() {
    QuerySubjects querySubjects = new QuerySubjects();
    querySubjects.setSubjects(new QuerySubjectArray(new QuerySubject().setEntity(SUBJECT_DATASET)));

    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());
    when(aspectRetriever.getLatestAspectObjects(
            any(),
            eq(Set.of(QUERY_URN)),
            eq(Set.of(Constants.QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                QUERY_URN,
                Map.of(
                    Constants.QUERY_SUBJECTS_ASPECT_NAME,
                    new Aspect(querySubjects.data()))));
    return aspectRetriever;
  }

  private static OperationContext createUserContextWithViewAuth(
      Authorizer authorizer, AspectRetriever aspectRetriever) {
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "datahub"), "");

    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(
                TestOperationContexts.emptyActiveUsersAspectRetriever(
                    aspectRetriever::getEntityRegistry))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .build();

    OperationContext systemContext =
        TestOperationContexts.systemContext(
            () ->
                OperationContextConfig.builder()
                    .viewAuthorizationConfiguration(
                        ViewAuthorizationConfiguration.builder().enabled(true).build())
                    .build(),
            null,
            null,
            null,
            () -> retrieverContext,
            null,
            null,
            null);

    return systemContext.asSession(RequestContext.TEST, authorizer, userAuth);
  }
}
