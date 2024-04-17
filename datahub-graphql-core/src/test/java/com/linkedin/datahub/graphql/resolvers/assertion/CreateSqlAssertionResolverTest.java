package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActionArray;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionActionInput;
import com.linkedin.datahub.graphql.generated.AssertionActionType;
import com.linkedin.datahub.graphql.generated.AssertionActionsInput;
import com.linkedin.datahub.graphql.generated.AssertionStdOperator;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterInput;
import com.linkedin.datahub.graphql.generated.AssertionStdParameterType;
import com.linkedin.datahub.graphql.generated.AssertionStdParametersInput;
import com.linkedin.datahub.graphql.generated.AssertionValueChangeType;
import com.linkedin.datahub.graphql.generated.CreateSqlAssertionInput;
import com.linkedin.datahub.graphql.generated.SqlAssertionType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.AssertionService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateSqlAssertionResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");

  private static final CreateSqlAssertionInput TEST_INPUT =
      new CreateSqlAssertionInput(
          TEST_DATASET_URN.toString(),
          "Test assertion description",
          SqlAssertionType.METRIC_CHANGE,
          "SELECT COUNT(*) FROM table WHERE some_condition = True",
          AssertionValueChangeType.ABSOLUTE,
          AssertionStdOperator.EQUAL_TO,
          new AssertionStdParametersInput(
              new AssertionStdParameterInput("100", AssertionStdParameterType.NUMBER), null, null),
          new AssertionActionsInput(
              ImmutableList.of(new AssertionActionInput(AssertionActionType.RESOLVE_INCIDENT)),
              ImmutableList.of(new AssertionActionInput(AssertionActionType.RAISE_INCIDENT))));

  private static final AssertionInfo TEST_ASSERTION_INFO =
      new AssertionInfo()
          .setType(AssertionType.SQL)
          .setDescription("Test assertion description")
          .setSqlAssertion(
              new SqlAssertionInfo()
                  .setEntity(TEST_DATASET_URN)
                  .setType(com.linkedin.assertion.SqlAssertionType.METRIC_CHANGE)
                  .setStatement("SELECT COUNT(*) FROM table WHERE some_condition = True")
                  .setChangeType(com.linkedin.assertion.AssertionValueChangeType.ABSOLUTE)
                  .setOperator(com.linkedin.assertion.AssertionStdOperator.EQUAL_TO)
                  .setParameters(
                      new com.linkedin.assertion.AssertionStdParameters()
                          .setValue(
                              new com.linkedin.assertion.AssertionStdParameter()
                                  .setValue("100")
                                  .setType(
                                      com.linkedin.assertion.AssertionStdParameterType.NUMBER))));

  private static final AssertionActions TEST_ASSERTION_ACTIONS =
      new AssertionActions()
          .setOnSuccess(
              new AssertionActionArray(
                  ImmutableList.of(
                      new AssertionAction()
                          .setType(com.linkedin.assertion.AssertionActionType.RESOLVE_INCIDENT))))
          .setOnFailure(
              new AssertionActionArray(
                  ImmutableList.of(
                      new AssertionAction()
                          .setType(com.linkedin.assertion.AssertionActionType.RAISE_INCIDENT))));

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    AssertionService mockService = initMockService();
    CreateSqlAssertionResolver resolver = new CreateSqlAssertionResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Assertion assertion = resolver.get(mockEnv).get();

    // Don't validate each field since we have mapper tests already.
    assertNotNull(assertion);
    assertEquals(assertion.getUrn(), TEST_ASSERTION_URN.toString());

    // Validate that we created the assertion
    Mockito.verify(mockService, Mockito.times(1))
        .createSqlAssertion(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_INFO.getSqlAssertion().getEntity()),
            Mockito.eq(TEST_ASSERTION_INFO.getSqlAssertion().getType()),
            Mockito.eq(TEST_ASSERTION_INFO.getDescription()),
            Mockito.eq(TEST_ASSERTION_INFO.getSqlAssertion()),
            Mockito.eq(TEST_ASSERTION_ACTIONS));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AssertionService mockService = Mockito.mock(AssertionService.class);
    CreateSqlAssertionResolver resolver = new CreateSqlAssertionResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(any(OperationContext.class), Mockito.any());
  }

  @Test
  public void testGetAssertionServiceException() throws Exception {
    // Create resolver
    AssertionService mockService = Mockito.mock(AssertionService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .createSqlAssertion(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any());

    CreateSqlAssertionResolver resolver = new CreateSqlAssertionResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private AssertionService initMockService() {
    AssertionService service = Mockito.mock(AssertionService.class);
    Mockito.when(
            service.createSqlAssertion(
                any(OperationContext.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()))
        .thenReturn(TEST_ASSERTION_URN);

    Mockito.when(
            service.getAssertionEntityResponse(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(
            new EntityResponse()
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(TEST_ASSERTION_INFO.data())),
                            Constants.ASSERTION_ACTIONS_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(TEST_ASSERTION_ACTIONS.data())))))
                .setEntityName(Constants.ASSERTION_ENTITY_NAME)
                .setUrn(TEST_ASSERTION_URN));
    return service;
  }
}
