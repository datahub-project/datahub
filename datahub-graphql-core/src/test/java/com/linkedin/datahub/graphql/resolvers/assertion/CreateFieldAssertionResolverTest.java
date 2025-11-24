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
import com.linkedin.assertion.FieldAssertionInfo;
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
import com.linkedin.datahub.graphql.generated.CreateFieldAssertionInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterInput;
import com.linkedin.datahub.graphql.generated.DatasetFilterType;
import com.linkedin.datahub.graphql.generated.FieldAssertionType;
import com.linkedin.datahub.graphql.generated.FieldMetricAssertionInput;
import com.linkedin.datahub.graphql.generated.FieldMetricType;
import com.linkedin.datahub.graphql.generated.SchemaFieldSpecInput;
import com.linkedin.dataset.DatasetFilter;
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

public class CreateFieldAssertionResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");

  private static final CreateFieldAssertionInput TEST_INPUT =
      new CreateFieldAssertionInput(
          FieldAssertionType.FIELD_METRIC,
          TEST_DATASET_URN.toString(),
          null,
          new FieldMetricAssertionInput(
              new SchemaFieldSpecInput("path", "INTEGER", "INTEGER"),
              FieldMetricType.UNIQUE_COUNT,
              AssertionStdOperator.EQUAL_TO,
              new AssertionStdParametersInput(
                  new AssertionStdParameterInput("100", AssertionStdParameterType.NUMBER),
                  null,
                  null)),
          new DatasetFilterInput(DatasetFilterType.SQL, "some_condition = True"),
          new AssertionActionsInput(
              ImmutableList.of(new AssertionActionInput(AssertionActionType.RESOLVE_INCIDENT)),
              ImmutableList.of(new AssertionActionInput(AssertionActionType.RAISE_INCIDENT))));

  private static final AssertionInfo TEST_ASSERTION_INFO =
      new AssertionInfo()
          .setType(AssertionType.FIELD)
          .setFieldAssertion(
              new FieldAssertionInfo()
                  .setEntity(TEST_DATASET_URN)
                  .setType(com.linkedin.assertion.FieldAssertionType.FIELD_METRIC)
                  .setFilter(
                      new DatasetFilter()
                          .setType(com.linkedin.dataset.DatasetFilterType.SQL)
                          .setSql("some_condition = True"))
                  .setFieldMetricAssertion(
                      new com.linkedin.assertion.FieldMetricAssertion()
                          .setField(
                              new com.linkedin.schema.SchemaFieldSpec()
                                  .setPath("path")
                                  .setType("INTEGER")
                                  .setNativeType("INTEGER"))
                          .setMetric(com.linkedin.assertion.FieldMetricType.UNIQUE_COUNT)
                          .setOperator(com.linkedin.assertion.AssertionStdOperator.EQUAL_TO)
                          .setParameters(
                              new com.linkedin.assertion.AssertionStdParameters()
                                  .setValue(
                                      new com.linkedin.assertion.AssertionStdParameter()
                                          .setValue("100")
                                          .setType(
                                              com.linkedin.assertion.AssertionStdParameterType
                                                  .NUMBER)))));

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
    CreateFieldAssertionResolver resolver = new CreateFieldAssertionResolver(mockService);

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
        .createFieldAssertion(
            any(OperationContext.class),
            Mockito.eq(TEST_ASSERTION_INFO.getFieldAssertion().getEntity()),
            Mockito.eq(TEST_ASSERTION_INFO.getFieldAssertion()),
            Mockito.eq(TEST_ASSERTION_ACTIONS),
            Mockito.eq(Constants.METADATA_TESTS_SOURCE));
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    AssertionService mockService = Mockito.mock(AssertionService.class);
    CreateFieldAssertionResolver resolver = new CreateFieldAssertionResolver(mockService);

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
        .createFieldAssertion(
            any(OperationContext.class),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(Constants.METADATA_TESTS_SOURCE));

    CreateFieldAssertionResolver resolver = new CreateFieldAssertionResolver(mockService);

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
            service.createFieldAssertion(
                any(OperationContext.class),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.eq(Constants.METADATA_TESTS_SOURCE)))
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
