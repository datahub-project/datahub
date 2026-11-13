package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.lifecycle.LifecycleStageSettings;
import com.linkedin.lifecycle.LifecycleStageTransitionPolicy;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SetLifecycleStageResolverTest {

  private static final String ENTITY_URN = "urn:li:dataset:(urn:li:dataPlatform:hdfs,mydata,PROD)";
  private static final String STAGE_URN = "urn:li:lifecycleStageType:DRAFT";
  private static final String ACTOR_URN = "urn:li:corpuser:testUser";

  @Test
  public void testSetLifecycleStage() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    mockAssignableStage(mockService);
    QueryContext mockContext = getMockAllowContext(ACTOR_URN);

    // Existing Status with removed=true to verify we preserve it
    Status existingStatus = new Status().setRemoved(true);
    Mockito.when(
            mockService.getAspect(
                any(), eq(UrnUtils.getUrn(ENTITY_URN)), eq(STATUS_ASPECT_NAME), eq(0L)))
        .thenReturn(existingStatus);

    DataFetchingEnvironment mockEnv = mockEnv(ENTITY_URN, STAGE_URN, mockContext);

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockService);
    assertTrue(resolver.get(mockEnv).get());

    // Cannot easily validate exact payload due to framework-injected SystemMetadata
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testClearLifecycleStage() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    mockAssignableStage(mockService);
    QueryContext mockContext = getMockAllowContext(ACTOR_URN);

    Status existingStatus =
        new Status().setRemoved(false).setLifecycleStage(UrnUtils.getUrn(STAGE_URN));
    Mockito.when(
            mockService.getAspect(
                any(), eq(UrnUtils.getUrn(ENTITY_URN)), eq(STATUS_ASPECT_NAME), eq(0L)))
        .thenReturn(existingStatus);

    DataFetchingEnvironment mockEnv = mockEnv(ENTITY_URN, null, mockContext);

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockService);
    assertTrue(resolver.get(mockEnv).get());

    // Cannot easily validate exact payload due to framework-injected SystemMetadata
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testSetLifecycleStageNoExistingStatus() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    mockAssignableStage(mockService);
    QueryContext mockContext = getMockAllowContext(ACTOR_URN);

    Mockito.when(mockService.getAspect(any(), any(Urn.class), eq(STATUS_ASPECT_NAME), eq(0L)))
        .thenReturn(null);

    DataFetchingEnvironment mockEnv = mockEnv(ENTITY_URN, STAGE_URN, mockContext);

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockService);
    assertTrue(resolver.get(mockEnv).get());

    // Cannot easily validate exact payload due to framework-injected SystemMetadata
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testClearLifecycleStageWithBlankString() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    QueryContext mockContext = getMockAllowContext(ACTOR_URN);

    Status existingStatus = new Status().setLifecycleStage(UrnUtils.getUrn(STAGE_URN));
    Mockito.when(
            mockService.getAspect(
                any(), eq(UrnUtils.getUrn(ENTITY_URN)), eq(STATUS_ASPECT_NAME), eq(0L)))
        .thenReturn(existingStatus);

    DataFetchingEnvironment mockEnv = mockEnv(ENTITY_URN, "   ", mockContext);

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockService);
    assertTrue(resolver.get(mockEnv).get());

    // Cannot easily validate exact payload due to framework-injected SystemMetadata
    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testSetLifecycleStageUnauthorized() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    QueryContext mockContext = getMockDenyContext();

    DataFetchingEnvironment mockEnv = mockEnv(ENTITY_URN, STAGE_URN, mockContext);

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockService);
    org.testng.Assert.assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    // No ingestion should happen when unauthorized
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testRejectsSystemManagedStage() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    LifecycleStageTypeInfo info =
        new LifecycleStageTypeInfo()
            .setName("System managed")
            .setSettings(new LifecycleStageSettings().setUserAssignable(false));
    Mockito.when(
            mockService.getAspect(
                any(), eq(UrnUtils.getUrn(STAGE_URN)), eq("lifecycleStageTypeInfo"), eq(0L)))
        .thenReturn(info);
    QueryContext mockContext = getMockAllowContext(ACTOR_URN);

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockService);
    CompletionException exception =
        org.testng.Assert.expectThrows(
            CompletionException.class,
            () -> resolver.get(mockEnv(ENTITY_URN, STAGE_URN, mockContext)).join());

    assertTrue(exception.getCause() instanceof DataHubGraphQLException);
    assertTrue(exception.getCause().getMessage().contains("system-managed"));
    Mockito.verify(mockService, Mockito.never()).ingestProposal(any(), any(), anyBoolean());
  }

  private static DataFetchingEnvironment mockEnv(
      String urn, String lifecycleStageUrn, QueryContext context) {
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(context);
    Mockito.when(mockEnv.getArgument("urn")).thenReturn(urn);
    Mockito.when(mockEnv.getArgument("lifecycleStageUrn")).thenReturn(lifecycleStageUrn);
    return mockEnv;
  }

  private static void mockAssignableStage(EntityService<?> service) throws Exception {
    LifecycleStageTypeInfo info =
        new LifecycleStageTypeInfo()
            .setName("Draft")
            .setSettings(new LifecycleStageSettings().setUserAssignable(true));
    Mockito.when(
            service.getAspect(
                any(), eq(UrnUtils.getUrn(STAGE_URN)), eq("lifecycleStageTypeInfo"), eq(0L)))
        .thenReturn(info);
  }

  private static void mockStageWithPolicy(EntityService<?> service, String... allowedPrevious)
      throws Exception {
    LifecycleStageTypeInfo info =
        new LifecycleStageTypeInfo()
            .setName("Draft")
            .setSettings(new LifecycleStageSettings().setUserAssignable(true))
            .setTransitionPolicy(
                new LifecycleStageTransitionPolicy()
                    .setAllowedPreviousStages(
                        new UrnArray(
                            java.util.Arrays.stream(allowedPrevious)
                                .map(UrnUtils::getUrn)
                                .toList())));
    Mockito.when(
            service.getAspect(
                any(), eq(UrnUtils.getUrn(STAGE_URN)), eq("lifecycleStageTypeInfo"), eq(0L)))
        .thenReturn(info);
  }

  @Test
  public void testRejectsDisallowedTransition() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    mockStageWithPolicy(mockService, "urn:li:lifecycleStageType:IN_REVIEW");
    Mockito.when(
            mockService.getAspect(
                any(), eq(UrnUtils.getUrn(ENTITY_URN)), eq(STATUS_ASPECT_NAME), eq(0L)))
        .thenReturn(
            new Status().setLifecycleStage(UrnUtils.getUrn("urn:li:lifecycleStageType:ARCHIVED")));
    QueryContext mockContext = getMockAllowContext(ACTOR_URN);

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockService);
    CompletionException exception =
        org.testng.Assert.expectThrows(
            CompletionException.class,
            () -> resolver.get(mockEnv(ENTITY_URN, STAGE_URN, mockContext)).join());

    assertTrue(exception.getCause() instanceof DataHubGraphQLException);
    assertTrue(exception.getCause().getMessage().contains("is not allowed"));
    Mockito.verify(mockService, Mockito.never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testAllowsEntryFromNoStageViaSentinel() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    mockStageWithPolicy(mockService, "urn:li:lifecycleStageType:__NONE__");
    QueryContext mockContext = getMockAllowContext(ACTOR_URN);

    SetLifecycleStageResolver resolver = new SetLifecycleStageResolver(mockService);
    assertTrue(resolver.get(mockEnv(ENTITY_URN, STAGE_URN, mockContext)).get());
    verifyIngestProposal(mockService, 1);
  }
}
