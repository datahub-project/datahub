package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionActionsInput;
import com.linkedin.datahub.graphql.generated.MonitorMode;
import com.linkedin.datahub.graphql.generated.SchemaAssertionCompatibility;
import com.linkedin.datahub.graphql.generated.SchemaAssertionInput;
import com.linkedin.datahub.graphql.generated.SchemaFieldDataType;
import com.linkedin.datahub.graphql.generated.UpsertDatasetSchemaAssertionMonitorInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationParametersType;
import com.linkedin.monitor.DatasetSchemaAssertionParameters;
import com.linkedin.monitor.DatasetSchemaSourceType;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.Schemaless;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.LongSupplier;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpsertDatasetSchemaAssertionMonitorResolverTest {

  private static final String TEST_ASSERTION_URN = "urn:li:assertion:1";
  private static final String TEST_DATASET_URN = "urn:li:dataset:1";
  private static final String TEST_MONITOR_URN = "urn:li:monitor:1";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:1";

  @Mock private static LongSupplier timeProvider = () -> 1000L;

  @Mock private AssertionService assertionService;

  @Mock private MonitorService monitorService;

  @Mock private GraphClient graphClient;

  @Mock private DataFetchingEnvironment dataFetchingEnvironment;

  @InjectMocks private UpsertDatasetSchemaAssertionMonitorResolver resolver;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testGetCreateAssertion() throws Exception {

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Arrange
    UpsertDatasetSchemaAssertionMonitorInput input = new UpsertDatasetSchemaAssertionMonitorInput();
    input.setEntityUrn(TEST_DATASET_URN);
    input.setDescription("description");
    input.setMode(MonitorMode.ACTIVE);
    input.setAssertion(
        new SchemaAssertionInput(
            SchemaAssertionCompatibility.SUPERSET,
            ImmutableList.of(
                new com.linkedin.datahub.graphql.generated.SchemaAssertionFieldInput(
                    "field1", SchemaFieldDataType.DATE, "nativeDate"),
                new com.linkedin.datahub.graphql.generated.SchemaAssertionFieldInput(
                    "field2", SchemaFieldDataType.BOOLEAN, "nativeBoolean"))));
    input.setActions(new AssertionActionsInput(Collections.emptyList(), Collections.emptyList()));
    when(dataFetchingEnvironment.getArgument("assertionUrn")).thenReturn(null);
    when(dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(assertionService.generateAssertionUrn())
        .thenReturn(Urn.createFromString(TEST_ASSERTION_URN));
    when(monitorService.generateMonitorUrn(any(Urn.class)))
        .thenReturn(Urn.createFromString(TEST_MONITOR_URN));

    when(assertionService.getAssertionEntityResponse(
            any(OperationContext.class), eq(UrnUtils.getUrn(TEST_ASSERTION_URN))))
        .thenReturn(getAssertionInfoEntityResponse());

    // Act
    UpsertDatasetSchemaAssertionMonitorResolver resolver =
        new UpsertDatasetSchemaAssertionMonitorResolver(
            assertionService, monitorService, graphClient, timeProvider);
    CompletableFuture<Assertion> future = resolver.get(dataFetchingEnvironment);
    Assertion assertion = future.get();

    // Assert
    assertNotNull(assertion);
    verify(assertionService, times(1))
        .upsertDatasetSchemaAssertion(
            Mockito.any(OperationContext.class),
            Mockito.eq(UrnUtils.getUrn(TEST_ASSERTION_URN)),
            Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
            Mockito.eq("description"),
            Mockito.eq(com.linkedin.assertion.SchemaAssertionCompatibility.SUPERSET),
            Mockito.any(SchemaMetadata.class),
            Mockito.any(AssertionActions.class),
            Mockito.any(AssertionSource.class));
    verify(monitorService, times(1))
        .upsertAssertionMonitor(
            any(OperationContext.class),
            Mockito.eq(UrnUtils.getUrn(TEST_MONITOR_URN)),
            Mockito.eq(UrnUtils.getUrn(TEST_ASSERTION_URN)),
            Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
            any(),
            Mockito.eq(
                new AssertionEvaluationParameters()
                    .setType(AssertionEvaluationParametersType.DATASET_SCHEMA)
                    .setDatasetSchemaParameters(
                        new DatasetSchemaAssertionParameters()
                            .setSourceType(DatasetSchemaSourceType.DATAHUB_SCHEMA))),
            Mockito.eq(com.linkedin.monitor.MonitorMode.ACTIVE),
            Mockito.eq(null));
  }

  @Test
  public void testGetUpdateAssertion() throws Exception {

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Arrange
    UpsertDatasetSchemaAssertionMonitorInput input = new UpsertDatasetSchemaAssertionMonitorInput();
    input.setEntityUrn(TEST_DATASET_URN);
    input.setDescription("new description");
    input.setMode(MonitorMode.ACTIVE);
    input.setAssertion(
        new SchemaAssertionInput(
            SchemaAssertionCompatibility.SUPERSET,
            ImmutableList.of(
                new com.linkedin.datahub.graphql.generated.SchemaAssertionFieldInput(
                    "field1", SchemaFieldDataType.DATE, "nativeDate"),
                new com.linkedin.datahub.graphql.generated.SchemaAssertionFieldInput(
                    "field2", SchemaFieldDataType.BOOLEAN, "nativeBoolean"))));
    input.setActions(new AssertionActionsInput(Collections.emptyList(), Collections.emptyList()));

    // Change the fields.
    String assertionUrn = TEST_ASSERTION_URN;
    when(dataFetchingEnvironment.getArgument("assertionUrn")).thenReturn(assertionUrn);
    when(dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATA_SCHEMA);
    assertionInfo.setDescription("new description");
    assertionInfo.setSchemaAssertion(
        new SchemaAssertionInfo()
            .setSchema(new SchemaMetadata())
            .setCompatibility(com.linkedin.assertion.SchemaAssertionCompatibility.SUPERSET)
            .setEntity(UrnUtils.getUrn(TEST_DATASET_URN)));
    assertionInfo.setSource(new AssertionSource().setType(AssertionSourceType.NATIVE));
    assertionInfo.setLastUpdated(
        new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn(TEST_ACTOR_URN)));
    when(assertionService.getAssertionInfo(
            any(OperationContext.class), Mockito.eq(UrnUtils.getUrn(assertionUrn))))
        .thenReturn(assertionInfo);
    when(graphClient.getRelatedEntities(
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(ImmutableSet.of("Evaluates")),
            Mockito.eq(RelationshipDirection.INCOMING),
            Mockito.eq(0),
            Mockito.eq(1),
            Mockito.any()))
        .thenReturn(
            new EntityRelationships()
                .setCount(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new com.linkedin.common.EntityRelationship()
                                .setEntity(Urn.createFromString(TEST_MONITOR_URN))))));

    when(assertionService.getAssertionEntityResponse(
            any(OperationContext.class), Mockito.eq(UrnUtils.getUrn(TEST_ASSERTION_URN))))
        .thenReturn(getAssertionInfoEntityResponse());

    // Act
    UpsertDatasetSchemaAssertionMonitorResolver resolver =
        new UpsertDatasetSchemaAssertionMonitorResolver(
            assertionService, monitorService, graphClient, timeProvider);
    CompletableFuture<Assertion> future = resolver.get(dataFetchingEnvironment);
    Assertion assertion = future.get();

    // Assert
    assertNotNull(assertion);
    verify(assertionService, times(1))
        .upsertDatasetSchemaAssertion(
            any(OperationContext.class),
            Mockito.eq(UrnUtils.getUrn(TEST_ASSERTION_URN)),
            Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
            Mockito.eq("new description"), // description was updated
            Mockito.eq(com.linkedin.assertion.SchemaAssertionCompatibility.SUPERSET),
            Mockito.any(SchemaMetadata.class),
            Mockito.any(AssertionActions.class),
            Mockito.any(AssertionSource.class));
    verify(monitorService, times(1))
        .upsertAssertionMonitor(
            any(OperationContext.class),
            Mockito.eq(UrnUtils.getUrn(TEST_MONITOR_URN)),
            Mockito.eq(UrnUtils.getUrn(TEST_ASSERTION_URN)),
            Mockito.eq(UrnUtils.getUrn(TEST_DATASET_URN)),
            any(),
            Mockito.eq(
                new AssertionEvaluationParameters()
                    .setType(AssertionEvaluationParametersType.DATASET_SCHEMA)
                    .setDatasetSchemaParameters(
                        new DatasetSchemaAssertionParameters()
                            .setSourceType(DatasetSchemaSourceType.DATAHUB_SCHEMA))),
            Mockito.eq(com.linkedin.monitor.MonitorMode.ACTIVE),
            Mockito.eq(null));
  }

  @Test
  public void testGetCreateAssertionWithoutEntityUrn() {
    // Arrange
    UpsertDatasetSchemaAssertionMonitorInput input = new UpsertDatasetSchemaAssertionMonitorInput();
    when(dataFetchingEnvironment.getArgument("assertionUrn")).thenReturn(null);
    final QueryContext mockContext = getMockAllowContext();
    when(dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // Assert
    // IllegalArgumentException should be thrown
    assertThrows(IllegalArgumentException.class, () -> resolver.get(dataFetchingEnvironment));
  }

  @Test
  public void testGetUpdateAssertionWithInvalidType() throws Exception {
    // Arrange
    UpsertDatasetSchemaAssertionMonitorInput input = new UpsertDatasetSchemaAssertionMonitorInput();
    input.setEntityUrn(TEST_DATASET_URN);
    String assertionUrn = TEST_ASSERTION_URN;
    when(dataFetchingEnvironment.getArgument("assertionUrn")).thenReturn(assertionUrn);
    when(dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    when(dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS); // Invalid type
    when(assertionService.getAssertionInfo(any(OperationContext.class), any(Urn.class)))
        .thenReturn(assertionInfo);

    // Assert
    // IllegalArgumentException should be thrown
    assertThrows(IllegalArgumentException.class, () -> resolver.get(dataFetchingEnvironment));
  }

  private EntityResponse getAssertionInfoEntityResponse() {

    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setSchemaName("assertion-schema-name");
    schemaMetadata.setVersion(0L);
    try {
      schemaMetadata.setPlatform(
          DataPlatformUrn.createFromUrn(UrnUtils.getUrn("urn:li:dataPlatform:datahub")));
    } catch (Exception e) {
      // ignored. should never happen.
    }
    schemaMetadata.setHash("assertion-schema-hash");
    schemaMetadata.setPlatformSchema(SchemaMetadata.PlatformSchema.create(new Schemaless()));
    schemaMetadata.setFields(new SchemaFieldArray());

    return new EntityResponse()
        .setUrn(UrnUtils.getUrn(TEST_ASSERTION_URN))
        .setEntityName(Constants.ASSERTION_ENTITY_NAME)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    Constants.ASSERTION_INFO_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setName(Constants.ASSERTION_INFO_ASPECT_NAME)
                        .setValue(
                            new Aspect(
                                new AssertionInfo()
                                    .setType(AssertionType.DATA_SCHEMA)
                                    .setSchemaAssertion(
                                        new SchemaAssertionInfo()
                                            .setEntity(UrnUtils.getUrn(TEST_DATASET_URN))
                                            .setCompatibility(
                                                com.linkedin.assertion.SchemaAssertionCompatibility
                                                    .SUPERSET)
                                            .setSchema(schemaMetadata))
                                    .data())))));
  }

  @Test
  public void testGetCreateAssertionMonitorLimitExceeded() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Arrange
    UpsertDatasetSchemaAssertionMonitorInput input = new UpsertDatasetSchemaAssertionMonitorInput();
    input.setEntityUrn(TEST_DATASET_URN);
    input.setDescription("description");
    input.setMode(MonitorMode.ACTIVE);
    input.setAssertion(
        new SchemaAssertionInput(SchemaAssertionCompatibility.SUPERSET, ImmutableList.of()));

    Mockito.when(mockEnv.getArgument(Mockito.eq("assertionUrn"))).thenReturn(null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    // Mock services
    AssertionService mockAssertionService = Mockito.mock(AssertionService.class);
    MonitorService mockMonitorService = Mockito.mock(MonitorService.class);
    GraphClient mockGraphClient = Mockito.mock(GraphClient.class);

    // Mock assertion service
    Mockito.when(mockAssertionService.generateAssertionUrn())
        .thenReturn(UrnUtils.getUrn(TEST_ASSERTION_URN));

    // Mock monitor service to throw limit exceeded exception
    Mockito.when(
            mockMonitorService.upsertAssertionMonitor(
                any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(
            new RuntimeException(AcrylConstants.MONITOR_LIMIT_EXCEEDED_ERROR_MESSAGE_PREFIX));

    UpsertDatasetSchemaAssertionMonitorResolver resolver =
        new UpsertDatasetSchemaAssertionMonitorResolver(
            mockAssertionService, mockMonitorService, mockGraphClient, timeProvider);

    CompletionException e =
        expectThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    assertTrue(e.getCause() instanceof DataHubGraphQLException);
    DataHubGraphQLException graphQLException = (DataHubGraphQLException) e.getCause();
    assertEquals(graphQLException.errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    assertTrue(graphQLException.getMessage().contains("Maximum number of monitors reached"));

    // Validate that we deleted the assertion
    Mockito.verify(mockAssertionService, Mockito.times(1))
        .tryDeleteAssertion(any(), Mockito.eq(UrnUtils.getUrn(TEST_ASSERTION_URN)));
  }
}
