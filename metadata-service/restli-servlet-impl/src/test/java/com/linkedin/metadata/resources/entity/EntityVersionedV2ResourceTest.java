package com.linkedin.metadata.resources.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anySet;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.parseq.Task;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.linkedin.common.urn.VersionedUrn;
import com.linkedin.metadata.authorization.PoliciesConfig;

/**
 * Regression coverage for the confirmed disclosure path where this resource performed only
 * generic entity-READ authorization and returned {@code getEntitiesVersionedV2}'s response
 * unmodified, with no call to {@code EntityAuthorizationUtils.completelyRedactUnauthorizedQuerySqlAspects}
 * anywhere in the file — unlike {@code EntityV2Resource}/{@code EntitiesController}, which both
 * redact before returning. That meant any actor with plain view access to a dataset/chart/data job
 * could read a restricted {@code viewProperties}/{@code dataTransformLogic}/{@code chartQuery}
 * aspect for any explicit version stamp.
 */
public class EntityVersionedV2ResourceTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
  private static final Urn USER_URN = UrnUtils.getUrn("urn:li:corpuser:victim");

  private EntityVersionedV2Resource resource;
  private EntityService<?> entityService;
  private Engine parseqEngine;

  @BeforeMethod
  public void setup() {
    resource = new EntityVersionedV2Resource();
    entityService = mock(EntityService.class);
    Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "allowed");
            });

    resource.setEntityService(entityService);
    resource.setAuthorizer(authorizer);
    resource.setSystemOperationContext(TestOperationContexts.systemContextNoSearchAuthorization());

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);

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

  @Test
  public void testBatchGetVersionedRedactsRestrictedQuerySqlAspect() throws Exception {
    EntityResponse response = entityResponseWithViewProperties(DATASET_URN);
    when(entityService.getEntitiesVersionedV2(any(OperationContext.class), any(), any()))
        .thenReturn(Map.of(DATASET_URN, response));

    try (MockedStatic<EntityAuthorizationUtils> mockedUtils =
        Mockito.mockStatic(EntityAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mockedUtils
          .when(
              () ->
                  EntityAuthorizationUtils.isQuerySqlAspectRestricted(
                      any(OperationContext.class),
                      eq(DATASET_URN),
                      eq(Constants.VIEW_PROPERTIES_ASPECT_NAME)))
          .thenReturn(true);

      Map<Urn, EntityResponse> result =
          awaitTask(
              resource.batchGetVersioned(
                  Set.of(new com.linkedin.common.urn.VersionedUrn(DATASET_URN.toString(), null)),
                  Constants.DATASET_ENTITY_NAME,
                  null));

      assertFalse(
          result.get(DATASET_URN).getAspects().containsKey(Constants.VIEW_PROPERTIES_ASPECT_NAME),
          "a restricted viewProperties aspect must be redacted before this endpoint returns it");
    }
  }

  @Test
  public void testBatchGetVersionedKeepsAllowedQuerySqlAspect() throws Exception {
    EntityResponse response = entityResponseWithViewProperties(DATASET_URN);
    when(entityService.getEntitiesVersionedV2(any(OperationContext.class), any(), any()))
        .thenReturn(Map.of(DATASET_URN, response));

    try (MockedStatic<EntityAuthorizationUtils> mockedUtils =
        Mockito.mockStatic(EntityAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mockedUtils
          .when(
              () ->
                  EntityAuthorizationUtils.isQuerySqlAspectRestricted(
                      any(OperationContext.class),
                      eq(DATASET_URN),
                      eq(Constants.VIEW_PROPERTIES_ASPECT_NAME)))
          .thenReturn(false);

      Map<Urn, EntityResponse> result =
          awaitTask(
              resource.batchGetVersioned(
                  Set.of(new com.linkedin.common.urn.VersionedUrn(DATASET_URN.toString(), null)),
                  Constants.DATASET_ENTITY_NAME,
                  null));

      assertTrue(
          result.get(DATASET_URN).getAspects().containsKey(Constants.VIEW_PROPERTIES_ASPECT_NAME),
          "viewProperties must remain when the actor is authorized to view it");
    }
  }

  private static EntityResponse entityResponseWithViewProperties(Urn urn) {
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.VIEW_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setName(Constants.VIEW_PROPERTIES_ASPECT_NAME)
            .setValue(new Aspect()));
    return new EntityResponse()
        .setUrn(urn)
        .setEntityName(Constants.DATASET_ENTITY_NAME)
        .setAspects(aspects);
  }

  @Test
  public void testBatchGetVersionedOmitsCredentialsWithoutManageUserCredentials()
      throws Exception {
    EntityService<?> entityService = mock(EntityService.class);
    when(entityService.getEntitiesVersionedV2(any(), anySet(), anySet()))
        .thenReturn(
            Map.of(USER_URN, EntityV2ResourceTest.corpUserResponseWithCredentials(USER_URN)));

    Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              AuthorizationResult.Type type =
                  PoliciesConfig.MANAGE_USER_CREDENTIALS_PRIVILEGE
                          .getType()
                          .equals(request.getPrivilege())
                      ? AuthorizationResult.Type.DENY
                      : AuthorizationResult.Type.ALLOW;
              return new AuthorizationResult(request, type, "");
            });

    EntityVersionedV2Resource resource = new EntityVersionedV2Resource();
    resource.setEntityService(entityService);
    resource.setAuthorizer(authorizer);
    resource.setSystemOperationContext(TestOperationContexts.systemContextNoSearchAuthorization());
    AuthenticationContext.setAuthentication(
        new Authentication(new Actor(ActorType.USER, "regular-user"), ""));

    Map<Urn, EntityResponse> responses =
        EntityV2ResourceTest.awaitTask(
            resource.batchGetVersioned(
                Set.of(new VersionedUrn(USER_URN.toString(), null)), "corpuser", null));

    assertEquals(
        responses.get(USER_URN).getAspects().keySet(),
        Set.of(Constants.CORP_USER_INFO_ASPECT_NAME));
  }
}
