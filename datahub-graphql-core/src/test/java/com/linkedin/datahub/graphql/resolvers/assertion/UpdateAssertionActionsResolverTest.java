package com.linkedin.datahub.graphql.resolvers.assertion;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActionArray;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.FreshnessCronSchedule;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionActionInput;
import com.linkedin.datahub.graphql.generated.AssertionActionType;
import com.linkedin.datahub.graphql.generated.AssertionActionsInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class UpdateAssertionActionsResolverTest {

  private static final Urn TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");

  private static final AssertionActionsInput TEST_INPUT = new AssertionActionsInput(
      ImmutableList.of(new AssertionActionInput(AssertionActionType.RESOLVE_INCIDENT)),
      ImmutableList.of(new AssertionActionInput(AssertionActionType.RAISE_INCIDENT))
  );

  private static final AssertionInfo TEST_ASSERTION_INFO = new AssertionInfo()
      .setType(AssertionType.FRESHNESS)
      .setFreshnessAssertion(
          new FreshnessAssertionInfo()
              .setEntity(TEST_DATASET_URN)
              .setType(FreshnessAssertionType.DATASET_CHANGE)
              .setSchedule(new FreshnessAssertionSchedule()
                  .setType(com.linkedin.assertion.FreshnessAssertionScheduleType.CRON)
                  .setCron(new FreshnessCronSchedule()
                      .setCron("* * * * *")
                      .setTimezone("America / Los Angeles")
                  )
              )
      );

  private static final AssertionActions TEST_ASSERTION_ACTIONS = new AssertionActions()
      .setOnSuccess(new AssertionActionArray(ImmutableList.of(new AssertionAction().setType(
          com.linkedin.assertion.AssertionActionType.RESOLVE_INCIDENT))))
      .setOnFailure(new AssertionActionArray(ImmutableList.of(new AssertionAction().setType(
          com.linkedin.assertion.AssertionActionType.RAISE_INCIDENT))));

  @Test
  public void testGetSuccess() throws Exception {
    // Update resolver
    AssertionService mockService = initMockService();
    UpdateAssertionActionsResolver resolver = new UpdateAssertionActionsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assertion assertion = resolver.get(mockEnv).get();

    // Don't validate each field since we have mapper tests already.
    assertNotNull(assertion);
    assertEquals(assertion.getUrn(), TEST_ASSERTION_URN.toString());

    // Validate that we created the assertion
    Mockito.verify(mockService, Mockito.times(1)).updateAssertionActions(
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.eq(TEST_ASSERTION_ACTIONS),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Update resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AssertionService mockService = Mockito.mock(AssertionService.class);
    UpdateAssertionActionsResolver resolver = new UpdateAssertionActionsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
  }


  @Test
  public void testGetAssertionDoesNotExistException() throws Exception {
    // Update resolver
    AssertionService mockService = Mockito.mock(AssertionService.class);
    Mockito.when(mockService.getAssertionEntityResponse(
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.any(Authentication.class)
    )).thenReturn(new EntityResponse()
        .setAspects(new EnvelopedAspectMap(Collections.emptyMap()))
        .setEntityName(Constants.ASSERTION_ENTITY_NAME)
        .setUrn(TEST_ASSERTION_URN)
    );

    UpdateAssertionActionsResolver resolver = new UpdateAssertionActionsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetAssertionServiceException() throws Exception {
    // Update resolver
    AssertionService mockService = Mockito.mock(AssertionService.class);
    Mockito.doThrow(RuntimeException.class).when(mockService).updateAssertionActions(
        Mockito.any(),
        Mockito.any(),
        Mockito.any(Authentication.class));

    UpdateAssertionActionsResolver resolver = new UpdateAssertionActionsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_ASSERTION_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private AssertionService initMockService() {
    AssertionService service = Mockito.mock(AssertionService.class);
    Mockito.when(service.updateFreshnessAssertion(
        Mockito.any(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any(Authentication.class)
    )).thenReturn(TEST_ASSERTION_URN);

    Mockito.when(service.getAssertionEntityResponse(
        Mockito.eq(TEST_ASSERTION_URN),
        Mockito.any(Authentication.class)
    )).thenReturn(new EntityResponse()
        .setAspects(new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.ASSERTION_INFO_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(TEST_ASSERTION_INFO.data())),
                Constants.ASSERTION_ACTIONS_ASPECT_NAME,
                new EnvelopedAspect().setValue(new Aspect(TEST_ASSERTION_ACTIONS.data()))
            )
        ))
        .setEntityName(Constants.ASSERTION_ENTITY_NAME)
        .setUrn(TEST_ASSERTION_URN)
    );

    Mockito.when(service.getAssertionInfo(
        Mockito.eq(TEST_ASSERTION_URN)
    )).thenReturn(TEST_ASSERTION_INFO);

    return service;
  }
}