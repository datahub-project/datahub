package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.service.AssertionService.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionAction;
import com.linkedin.assertion.AssertionActionArray;
import com.linkedin.assertion.AssertionActionType;
import com.linkedin.assertion.AssertionActions;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultError;
import com.linkedin.assertion.AssertionResultErrorType;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionStdAggregation;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionStdParameter;
import com.linkedin.assertion.AssertionStdParameterType;
import com.linkedin.assertion.AssertionStdParameters;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.AssertionValueChangeType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FieldMetricAssertion;
import com.linkedin.assertion.FieldMetricType;
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.RowCountTotal;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetFilter;
import com.linkedin.dataset.DatasetFilterType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.builder.AssertionRunSummaryPatchBuilder;
import com.linkedin.metadata.aspect.patch.builder.AssertionsSummaryPatchBuilder;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.schema.SchemaFieldSpec;
import com.linkedin.timeseries.CalendarInterval;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AssertionServiceTest {

  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_FRESHNESS_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:test-dataset-freshness");

  private static final Urn TEST_VOLUME_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:test-dataset-volume");

  private static final Urn TEST_SQL_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:test-dataset-sql");

  private static final Urn TEST_FIELD_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:test-dataset-field");
  private static final Urn TEST_NON_EXISTENT_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:test-non-existant");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");

  private static final Urn TEST_FIELD_URN =
      UrnUtils.getUrn(
          "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD),field1)");
  private static final Urn TEST_NON_EXISTENT_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,non-existant,PROD)");
  private static final Urn TEST_PLATFORM_URN = UrnUtils.getUrn("urn:li:dataPlatform:hive");

  private static final Urn TEST_PLATFORM_INSTANCE_URN =
      UrnUtils.getUrn("urn:li:dataPlatformInstance:(urn:li:dataPlatform:custom,instance1)");

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private OperationContext opContext;

  @BeforeTest
  public void setup() {
    opContext = TestOperationContexts.userContextNoSearchAuthorization(TEST_ACTOR_URN);
  }

  @Test
  private void testGetAssertionInfo() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Case 1: Info exists
    AssertionInfo info = service.getAssertionInfo(opContext, TEST_ASSERTION_URN);
    Assert.assertEquals(info, mockAssertionInfo());
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    Constants.ASSERTION_INFO_ASPECT_NAME,
                    Constants.ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    Constants.GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));

    // Case 2: Info does not exist
    info = service.getAssertionInfo(opContext, TEST_NON_EXISTENT_ASSERTION_URN);
    Assert.assertNull(info);
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_NON_EXISTENT_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    Constants.ASSERTION_INFO_ASPECT_NAME,
                    Constants.ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    Constants.GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));
  }

  @Test
  private void testGetAssertionDataPlatformInstance() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Case 1: data platform exists
    DataPlatformInstance instance =
        service.getAssertionDataPlatformInstance(opContext, TEST_ASSERTION_URN);
    Assert.assertEquals(instance, mockDataPlatformInstance());
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    Constants.ASSERTION_INFO_ASPECT_NAME,
                    Constants.ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    Constants.GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));

    // Case 2: data platform info does not exist
    instance = service.getAssertionDataPlatformInstance(opContext, TEST_NON_EXISTENT_ASSERTION_URN);
    Assert.assertNull(instance);
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
            Mockito.eq(TEST_NON_EXISTENT_ASSERTION_URN),
            Mockito.eq(
                ImmutableSet.of(
                    Constants.ASSERTION_INFO_ASPECT_NAME,
                    Constants.ASSERTION_ACTIONS_ASPECT_NAME,
                    DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                    Constants.GLOBAL_TAGS_ASPECT_NAME,
                    ASSERTION_RUN_SUMMARY_ASPECT_NAME)));
  }

  @Test
  private void testGetAssertionsSummary() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Case 1: Summary exists
    AssertionsSummary summary = service.getAssertionsSummary(opContext, TEST_DATASET_URN);
    Assert.assertEquals(summary, mockAssertionSummary());
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(DATASET_ENTITY_NAME),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(ImmutableSet.of(ASSERTIONS_SUMMARY_ASPECT_NAME)));

    // Case 2: Summary does not exist
    summary = service.getAssertionsSummary(opContext, TEST_NON_EXISTENT_DATASET_URN);
    Assert.assertNull(summary);
    Mockito.verify(mockClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(ImmutableSet.of(Constants.ASSERTIONS_SUMMARY_ASPECT_NAME)));
  }

  @Test
  private void testGetAssertionRunSummary() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Case 1: Summary exists
    AssertionRunSummary summary = service.getAssertionRunSummary(opContext, TEST_ASSERTION_URN);
    Assert.assertEquals(summary, mockAssertionRunSummary());

    // Case 2: Summary does not exist
    summary = service.getAssertionRunSummary(opContext, TEST_NON_EXISTENT_ASSERTION_URN);
    Assert.assertNull(summary);
  }

  @Test
  private void testUpdateAssertionsSummary() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();
    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);
    service.updateAssertionsSummary(opContext, TEST_DATASET_URN, mockAssertionSummary());
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), Mockito.eq(mockAssertionSummaryMcp()), Mockito.eq(false));
  }

  @Test
  public void testUpdateAssertionMetadata() throws Exception {
    final SystemEntityClient mockClient = createMockEntityClient();

    final AssertionInfo mockAssertionInfo = mockAssertionInfo();
    // Init for assertion info
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(
                    ImmutableSet.of(
                        Constants.ASSERTION_INFO_ASPECT_NAME,
                        ASSERTION_ACTIONS_ASPECT_NAME,
                        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                        Constants.GLOBAL_TAGS_ASPECT_NAME,
                        ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_ASSERTION_URN)
                .setEntityName(ASSERTION_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(mockAssertionInfo.data())),
                            DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockDataPlatformInstance().data()))))));

    final Instant nowInstant = Instant.now();
    final Clock clock = Clock.fixed(nowInstant, ZoneId.systemDefault());

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), clock, objectMapper);

    service.updateAssertionMetadata(opContext, TEST_ASSERTION_URN, mockAssertionActions(), null);
    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.eq(
                List.of(
                    mockAssertionActionsMcp(),
                    mockAssertionInfoLastUpdatedMcp(mockAssertionInfo, clock.millis()))),
            Mockito.eq(false));
  }

  // acryl-only
  @Test
  public void testCreateFreshnessAssertionRequiredFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:1");
    FreshnessAssertionType freshnessAssertionType = FreshnessAssertionType.DATASET_CHANGE;
    FreshnessAssertionSchedule schedule =
        new FreshnessAssertionSchedule()
            .setType(FreshnessAssertionScheduleType.FIXED_INTERVAL)
            .setFixedInterval(
                new FixedIntervalSchedule().setMultiple(2).setUnit(CalendarInterval.HOUR));
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.createFreshnessAssertion(
            opContext, entityUrn, freshnessAssertionType, schedule, null, null);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testCreateFreshnessAssertionAllFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:1");
    FreshnessAssertionType freshnessAssertionType = FreshnessAssertionType.DATASET_CHANGE;
    FreshnessAssertionSchedule schedule =
        new FreshnessAssertionSchedule()
            .setType(FreshnessAssertionScheduleType.FIXED_INTERVAL)
            .setFixedInterval(
                new FixedIntervalSchedule().setMultiple(2).setUnit(CalendarInterval.HOUR));
    DatasetFilter filter =
        new DatasetFilter().setType(DatasetFilterType.SQL).setSql("some_condition = True");
    AssertionActions actions =
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray());

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.createFreshnessAssertion(
            opContext, entityUrn, freshnessAssertionType, schedule, filter, actions);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testCreateVolumeAssertionRequiredFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:1");
    VolumeAssertionType volumeAssertionType = VolumeAssertionType.ROW_COUNT_TOTAL;
    RowCountTotal rowCountTotal =
        new RowCountTotal()
            .setOperator(AssertionStdOperator.EQUAL_TO)
            .setParameters(new AssertionStdParameters());
    VolumeAssertionInfo info = new VolumeAssertionInfo().setRowCountTotal(rowCountTotal);

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.createVolumeAssertion(opContext, entityUrn, volumeAssertionType, info, null);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testCreateVolumeAssertionAllFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:1");
    VolumeAssertionType volumeAssertionType = VolumeAssertionType.ROW_COUNT_TOTAL;
    AssertionStdParameters parameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1"));
    RowCountTotal rowCountTotal =
        new RowCountTotal().setOperator(AssertionStdOperator.EQUAL_TO).setParameters(parameters);
    DatasetFilter filter =
        new DatasetFilter().setType(DatasetFilterType.SQL).setSql("some_condition = True");
    VolumeAssertionInfo info =
        new VolumeAssertionInfo().setRowCountTotal(rowCountTotal).setFilter(filter);
    AssertionActions actions =
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray());

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.createVolumeAssertion(opContext, entityUrn, volumeAssertionType, info, actions);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testCreateDatasetAssertionRequiredFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:1");
    DatasetAssertionScope scope = DatasetAssertionScope.DATASET_ROWS;
    AssertionStdOperator operator = AssertionStdOperator.CONTAIN;

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.createDatasetAssertion(
            opContext, entityUrn, scope, null, null, operator, null, null);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testCreateDatasetAssertionAllFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Urn entityUrn = UrnUtils.getUrn("urn:li:dataset:1");
    DatasetAssertionScope scope = DatasetAssertionScope.DATASET_COLUMN;
    List<Urn> fields = ImmutableList.of();
    AssertionStdAggregation aggregation = AssertionStdAggregation.MAX;
    AssertionStdOperator operator = AssertionStdOperator.CONTAIN;
    AssertionStdParameters parameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1"));
    AssertionActions actions =
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray());

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.createDatasetAssertion(
            opContext, entityUrn, scope, fields, aggregation, operator, parameters, actions);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testUpsertDatasetFreshnessAssertionRequiredFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn assertionUrn = TEST_FRESHNESS_ASSERTION_URN;
    FreshnessAssertionSchedule schedule =
        new FreshnessAssertionSchedule()
            .setType(FreshnessAssertionScheduleType.FIXED_INTERVAL)
            .setFixedInterval(
                new FixedIntervalSchedule().setMultiple(2).setUnit(CalendarInterval.HOUR));
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              AssertionInfo newAssertionInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionInfo.class);
              Assert.assertEquals(newAssertionInfo.getType(), AssertionType.FRESHNESS);
              Assert.assertEquals(
                  newAssertionInfo.getFreshnessAssertion().getType(),
                  FreshnessAssertionType.DATASET_CHANGE);
              Assert.assertEquals(newAssertionInfo.getFreshnessAssertion().getSchedule(), schedule);
              Assert.assertEquals(
                  newAssertionInfo.getSource().getType(), AssertionSourceType.NATIVE);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.upsertDatasetFreshnessAssertion(
            opContext, assertionUrn, TEST_DATASET_URN, "description", schedule, null, null, null);

    // Assert result
    Assert.assertEquals(result, TEST_FRESHNESS_ASSERTION_URN);
  }

  @Test
  public void testUpsertDatasetFreshnessAssertionAllFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn assertionUrn = TEST_FRESHNESS_ASSERTION_URN;
    FreshnessAssertionSchedule schedule =
        new FreshnessAssertionSchedule()
            .setType(FreshnessAssertionScheduleType.FIXED_INTERVAL)
            .setFixedInterval(
                new FixedIntervalSchedule().setMultiple(2).setUnit(CalendarInterval.HOUR));
    DatasetFilter filter =
        new DatasetFilter().setType(DatasetFilterType.SQL).setSql("some_condition = True");
    AssertionActions actions =
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray());

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);
              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_INFO_ASPECT_NAME);

              // Verify that the correct aspects were ingested.
              AssertionInfo newAssertionInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionInfo.class);
              Assert.assertEquals(newAssertionInfo.getType(), AssertionType.FRESHNESS);
              Assert.assertEquals(
                  newAssertionInfo.getFreshnessAssertion().getType(),
                  FreshnessAssertionType.DATASET_CHANGE);
              Assert.assertEquals(newAssertionInfo.getFreshnessAssertion().getSchedule(), schedule);
              Assert.assertEquals(newAssertionInfo.getFreshnessAssertion().getFilter(), filter);
              Assert.assertEquals(
                  newAssertionInfo.getSource().getType(), AssertionSourceType.NATIVE);

              MetadataChangeProposal proposal2 = aspects.get(1);
              Assert.assertEquals(proposal2.getAspectName(), ASSERTION_ACTIONS_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              AssertionActions newAssertionActions =
                  GenericRecordUtils.deserializeAspect(
                      proposal2.getAspect().getValue(),
                      proposal2.getAspect().getContentType(),
                      AssertionActions.class);
              Assert.assertEquals(newAssertionActions, actions);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.upsertDatasetFreshnessAssertion(
            opContext,
            assertionUrn,
            TEST_DATASET_URN,
            "description",
            schedule,
            filter,
            actions,
            null);

    // Assert result
    Assert.assertEquals(result, TEST_FRESHNESS_ASSERTION_URN);
  }

  @Test
  public void testUpsertDatasetVolumeAssertionRequiredFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn assertionUrn = TEST_VOLUME_ASSERTION_URN;
    VolumeAssertionType volumeAssertionType = VolumeAssertionType.ROW_COUNT_TOTAL;
    RowCountTotal rowCountTotal =
        new RowCountTotal()
            .setOperator(AssertionStdOperator.EQUAL_TO)
            .setParameters(new AssertionStdParameters());
    VolumeAssertionInfo info =
        new VolumeAssertionInfo()
            .setRowCountTotal(rowCountTotal)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL);

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              AssertionInfo newAssertionInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionInfo.class);
              Assert.assertEquals(newAssertionInfo.getType(), AssertionType.VOLUME);
              Assert.assertEquals(
                  newAssertionInfo.getVolumeAssertion().getType(), volumeAssertionType);
              Assert.assertEquals(newAssertionInfo.getVolumeAssertion(), info);
              Assert.assertEquals(
                  newAssertionInfo.getSource().getType(), AssertionSourceType.NATIVE);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.upsertDatasetVolumeAssertion(
            opContext, assertionUrn, TEST_DATASET_URN, "description", info, null, null);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testUpsertDatasetVolumeAssertionAllFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn assertionUrn = TEST_VOLUME_ASSERTION_URN;
    VolumeAssertionType volumeAssertionType = VolumeAssertionType.ROW_COUNT_TOTAL;
    AssertionStdParameters parameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1"));
    RowCountTotal rowCountTotal =
        new RowCountTotal().setOperator(AssertionStdOperator.EQUAL_TO).setParameters(parameters);
    DatasetFilter filter =
        new DatasetFilter().setType(DatasetFilterType.SQL).setSql("some_condition = True");
    VolumeAssertionInfo info =
        new VolumeAssertionInfo()
            .setRowCountTotal(rowCountTotal)
            .setType(VolumeAssertionType.ROW_COUNT_TOTAL)
            .setFilter(filter);
    AssertionActions actions =
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray());

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);

              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              // Verify that the correct aspects were ingested.
              AssertionInfo newAssertionInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionInfo.class);
              Assert.assertEquals(newAssertionInfo.getType(), AssertionType.VOLUME);
              Assert.assertEquals(
                  newAssertionInfo.getVolumeAssertion().getType(), volumeAssertionType);
              Assert.assertEquals(newAssertionInfo.getVolumeAssertion(), info);
              Assert.assertEquals(
                  newAssertionInfo.getSource().getType(), AssertionSourceType.NATIVE);

              MetadataChangeProposal proposal2 = aspects.get(1);
              Assert.assertEquals(proposal2.getAspectName(), ASSERTION_ACTIONS_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              AssertionActions newAssertionActions =
                  GenericRecordUtils.deserializeAspect(
                      proposal2.getAspect().getValue(),
                      proposal2.getAspect().getContentType(),
                      AssertionActions.class);
              Assert.assertEquals(newAssertionActions, actions);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.upsertDatasetVolumeAssertion(
            opContext, assertionUrn, TEST_DATASET_URN, "description", info, actions, null);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testUpsertDatasetSqlAssertionRequiredFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn assertionUrn = TEST_SQL_ASSERTION_URN;
    SqlAssertionType sqlAssertionType = SqlAssertionType.METRIC;
    String description = "Test assertion description";
    AssertionStdParameters parameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1"));
    SqlAssertionInfo info =
        new SqlAssertionInfo()
            .setChangeType(AssertionValueChangeType.ABSOLUTE)
            .setType(sqlAssertionType)
            .setOperator(AssertionStdOperator.EQUAL_TO)
            .setParameters(parameters)
            .setEntity(TEST_DATASET_URN)
            .setStatement("SELECT COUNT(*) FROM table WHERE some_condition = True");

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);

              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              // Verify that the correct aspects were ingested.
              AssertionInfo newAssertionInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionInfo.class);
              Assert.assertEquals(newAssertionInfo.getType(), AssertionType.SQL);
              Assert.assertEquals(newAssertionInfo.getDescription(), description);
              Assert.assertEquals(newAssertionInfo.getSqlAssertion().getType(), sqlAssertionType);
              Assert.assertEquals(newAssertionInfo.getSqlAssertion(), info);
              Assert.assertEquals(
                  newAssertionInfo.getSource().getType(), AssertionSourceType.NATIVE);

              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.upsertDatasetSqlAssertion(
            opContext,
            assertionUrn,
            TEST_DATASET_URN,
            sqlAssertionType,
            description,
            info,
            null,
            null);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testUpsertDatasetSqlAssertionAllFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn assertionUrn = TEST_SQL_ASSERTION_URN;
    SqlAssertionType sqlAssertionType = SqlAssertionType.METRIC;
    String description = "Test assertion description";
    AssertionStdParameters parameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1"));
    SqlAssertionInfo info =
        new SqlAssertionInfo()
            .setChangeType(AssertionValueChangeType.ABSOLUTE)
            .setType(sqlAssertionType)
            .setOperator(AssertionStdOperator.EQUAL_TO)
            .setParameters(parameters)
            .setEntity(TEST_DATASET_URN)
            .setStatement("SELECT COUNT(*) FROM table WHERE some_condition = True");
    AssertionActions actions =
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray());

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);

              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              // Verify that the correct aspects were ingested.
              AssertionInfo newAssertionInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionInfo.class);
              Assert.assertEquals(newAssertionInfo.getType(), AssertionType.SQL);
              Assert.assertEquals(newAssertionInfo.getDescription(), description);
              Assert.assertEquals(newAssertionInfo.getSqlAssertion().getType(), sqlAssertionType);
              Assert.assertEquals(newAssertionInfo.getSqlAssertion(), info);
              Assert.assertEquals(
                  newAssertionInfo.getSource().getType(), AssertionSourceType.NATIVE);

              MetadataChangeProposal proposal2 = aspects.get(1);
              Assert.assertEquals(proposal2.getAspectName(), ASSERTION_ACTIONS_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              AssertionActions newAssertionActions =
                  GenericRecordUtils.deserializeAspect(
                      proposal2.getAspect().getValue(),
                      proposal2.getAspect().getContentType(),
                      AssertionActions.class);
              Assert.assertEquals(newAssertionActions, actions);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.upsertDatasetSqlAssertion(
            opContext,
            assertionUrn,
            TEST_DATASET_URN,
            sqlAssertionType,
            description,
            info,
            actions,
            null);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testUpsertDatasetFieldAssertionRequiredFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn assertionUrn = TEST_FIELD_ASSERTION_URN;

    DatasetFilter filter =
        new DatasetFilter().setType(DatasetFilterType.SQL).setSql("some_condition = True");
    AssertionStdParameters parameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1"));
    SchemaFieldSpec field =
        new SchemaFieldSpec().setPath("x").setType("NUMBER").setNativeType("NUMBER(38,0)");
    FieldAssertionType fieldAssertionType = FieldAssertionType.FIELD_METRIC;
    FieldAssertionInfo info =
        new FieldAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(fieldAssertionType)
            .setFilter(filter)
            .setFieldMetricAssertion(
                new FieldMetricAssertion()
                    .setField(field)
                    .setMetric(FieldMetricType.MAX)
                    .setOperator(AssertionStdOperator.EQUAL_TO)
                    .setParameters(parameters));

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);

              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              // Verify that the correct aspects were ingested.
              AssertionInfo newAssertionInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionInfo.class);
              Assert.assertEquals(newAssertionInfo.getType(), AssertionType.FIELD);

              Assert.assertEquals(
                  newAssertionInfo.getFieldAssertion().getType(), fieldAssertionType);
              Assert.assertEquals(newAssertionInfo.getFieldAssertion(), info);
              Assert.assertEquals(
                  newAssertionInfo.getSource().getType(), AssertionSourceType.NATIVE);

              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.upsertDatasetFieldAssertion(
            opContext, assertionUrn, TEST_DATASET_URN, "description", info, null, null);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testUpsertDatasetFieldAssertionAllFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn assertionUrn = TEST_SQL_ASSERTION_URN;

    DatasetFilter filter =
        new DatasetFilter().setType(DatasetFilterType.SQL).setSql("some_condition = True");
    AssertionStdParameters parameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1"));
    SchemaFieldSpec field =
        new SchemaFieldSpec().setPath("x").setType("NUMBER").setNativeType("NUMBER(38,0)");
    FieldAssertionType fieldAssertionType = FieldAssertionType.FIELD_METRIC;
    FieldAssertionInfo info =
        new FieldAssertionInfo()
            .setEntity(TEST_DATASET_URN)
            .setType(fieldAssertionType)
            .setFilter(filter)
            .setFieldMetricAssertion(
                new FieldMetricAssertion()
                    .setField(field)
                    .setMetric(FieldMetricType.MAX)
                    .setOperator(AssertionStdOperator.EQUAL_TO)
                    .setParameters(parameters));
    AssertionActions actions =
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray());

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);

              MetadataChangeProposal proposal = aspects.get(0);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              // Verify that the correct aspects were ingested.
              AssertionInfo newAssertionInfo =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionInfo.class);
              Assert.assertEquals(newAssertionInfo.getType(), AssertionType.FIELD);

              Assert.assertEquals(
                  newAssertionInfo.getFieldAssertion().getType(), fieldAssertionType);
              Assert.assertEquals(newAssertionInfo.getFieldAssertion(), info);
              Assert.assertEquals(
                  newAssertionInfo.getSource().getType(), AssertionSourceType.NATIVE);

              MetadataChangeProposal proposal2 = aspects.get(1);
              Assert.assertEquals(proposal2.getAspectName(), ASSERTION_ACTIONS_ASPECT_NAME);
              // Verify that the correct aspect was ingested.
              AssertionActions newAssertionActions =
                  GenericRecordUtils.deserializeAspect(
                      proposal2.getAspect().getValue(),
                      proposal2.getAspect().getContentType(),
                      AssertionActions.class);
              Assert.assertEquals(newAssertionActions, actions);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.upsertDatasetFieldAssertion(
            opContext, assertionUrn, TEST_DATASET_URN, "description", info, actions, null);

    // Assert result
    Assert.assertEquals(result.getEntityType(), "assertion");
  }

  @Test
  public void testUpsertCustomAssertionRequiredFields()
      throws Exception, RemoteInvocationException {
    SystemEntityClient mockedEntityClient = mock(SystemEntityClient.class);
    AssertionService assertionService =
        new AssertionService(
            mockedEntityClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    String descriptionOfCustomAssertion = "Description of custom assertion";
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);

              MetadataChangeProposal proposal1 = aspects.get(0);
              Assert.assertEquals(proposal1.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal1.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              AssertionInfo info =
                  GenericRecordUtils.deserializeAspect(
                      proposal1.getAspect().getValue(),
                      proposal1.getAspect().getContentType(),
                      AssertionInfo.class);

              Assert.assertEquals(info.getType(), AssertionType.CUSTOM);
              Assert.assertEquals(info.getDescription(), descriptionOfCustomAssertion);
              Assert.assertEquals(info.getSource().getType(), AssertionSourceType.EXTERNAL);
              CustomAssertionInfo customAssertionInfo = info.getCustomAssertion();
              Assert.assertEquals(customAssertionInfo.getEntity(), TEST_DATASET_URN);

              MetadataChangeProposal proposal2 = aspects.get(1);
              Assert.assertEquals(proposal2.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal2.getAspectName(), DATA_PLATFORM_INSTANCE_ASPECT_NAME);
              DataPlatformInstance dataPlatformInstance =
                  GenericRecordUtils.deserializeAspect(
                      proposal2.getAspect().getValue(),
                      proposal2.getAspect().getContentType(),
                      DataPlatformInstance.class);
              Assert.assertEquals(dataPlatformInstance.getPlatform(), TEST_PLATFORM_URN);
              return null;
            })
        .when(mockedEntityClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    Urn urn =
        assertionService.upsertCustomAssertion(
            opContext,
            TEST_ASSERTION_URN,
            TEST_DATASET_URN,
            descriptionOfCustomAssertion,
            null,
            new DataPlatformInstance().setPlatform(TEST_PLATFORM_URN),
            new CustomAssertionInfo().setEntity(TEST_DATASET_URN));
    Assert.assertEquals(urn.getEntityType(), "assertion");

    Mockito.verify(mockedEntityClient, Mockito.times(1))
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));
  }

  @Test
  public void testUpsertCustomAssertionAllFields() throws Exception, RemoteInvocationException {
    SystemEntityClient mockedEntityClient = mock(SystemEntityClient.class);
    AssertionService assertionService =
        new AssertionService(
            mockedEntityClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);
    String descriptionOfCustomAssertion = "Description of custom assertion";
    String externalUrlOfCustomAssertion = "https://xyz.com/abc";
    String customCategory = "Custom category";
    String customLogic = "select percentile(field1, 0.66) from table";
    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);

              MetadataChangeProposal proposal1 = aspects.get(0);
              Assert.assertEquals(proposal1.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal1.getAspectName(), ASSERTION_INFO_ASPECT_NAME);
              AssertionInfo info =
                  GenericRecordUtils.deserializeAspect(
                      proposal1.getAspect().getValue(),
                      proposal1.getAspect().getContentType(),
                      AssertionInfo.class);

              Assert.assertEquals(info.getType(), AssertionType.CUSTOM);
              Assert.assertEquals(info.getDescription(), descriptionOfCustomAssertion);
              Assert.assertEquals(info.getExternalUrl().toString(), externalUrlOfCustomAssertion);
              Assert.assertEquals(info.getSource().getType(), AssertionSourceType.EXTERNAL);
              CustomAssertionInfo customAssertionInfo = info.getCustomAssertion();
              Assert.assertEquals(customAssertionInfo.getEntity(), TEST_DATASET_URN);
              Assert.assertEquals(customAssertionInfo.getField(), TEST_FIELD_URN);
              Assert.assertEquals(customAssertionInfo.getType(), customCategory);
              Assert.assertEquals(customAssertionInfo.getLogic(), customLogic);

              MetadataChangeProposal proposal2 = aspects.get(1);
              Assert.assertEquals(proposal2.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal2.getAspectName(), DATA_PLATFORM_INSTANCE_ASPECT_NAME);
              DataPlatformInstance dataPlatformInstance =
                  GenericRecordUtils.deserializeAspect(
                      proposal2.getAspect().getValue(),
                      proposal2.getAspect().getContentType(),
                      DataPlatformInstance.class);
              Assert.assertEquals(dataPlatformInstance.getPlatform(), TEST_PLATFORM_URN);
              Assert.assertEquals(dataPlatformInstance.getInstance(), TEST_PLATFORM_INSTANCE_URN);
              return null;
            })
        .when(mockedEntityClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    Urn urn =
        assertionService.upsertCustomAssertion(
            opContext,
            TEST_ASSERTION_URN,
            TEST_DATASET_URN,
            descriptionOfCustomAssertion,
            externalUrlOfCustomAssertion,
            new DataPlatformInstance()
                .setPlatform(TEST_PLATFORM_URN)
                .setInstance(TEST_PLATFORM_INSTANCE_URN),
            new CustomAssertionInfo()
                .setEntity(TEST_DATASET_URN)
                .setField(TEST_FIELD_URN)
                .setType(customCategory)
                .setLogic(customLogic));
    Assert.assertEquals(urn.getEntityType(), "assertion");

    Mockito.verify(mockedEntityClient, Mockito.times(1))
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));
  }

  @Test
  public void testAddAssertionRunEventRequiredFields() throws Exception, RemoteInvocationException {
    SystemEntityClient mockedEntityClient = mock(SystemEntityClient.class);
    AssertionService assertionService =
        new AssertionService(
            mockedEntityClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);
    Long eventtime = 1718619000000L;

    Mockito.doAnswer(
            invocation -> {
              MetadataChangeProposal proposal = invocation.getArgument(1);

              Assert.assertEquals(proposal.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_RUN_EVENT_ASPECT_NAME);
              AssertionRunEvent runEvent =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionRunEvent.class);

              Assert.assertEquals(runEvent.getAssertionUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(runEvent.getAsserteeUrn(), TEST_DATASET_URN);
              Assert.assertEquals(runEvent.getTimestampMillis(), eventtime);
              Assert.assertEquals(runEvent.getStatus(), AssertionRunStatus.COMPLETE);

              AssertionResult result = runEvent.getResult();
              Assert.assertEquals(result.getType(), AssertionResultType.SUCCESS);

              return null;
            })
        .when(mockedEntityClient)
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));

    assertionService.addAssertionRunEvent(
        opContext,
        TEST_ASSERTION_URN,
        TEST_DATASET_URN,
        eventtime,
        new AssertionResult().setType(AssertionResultType.SUCCESS));

    Mockito.verify(mockedEntityClient, Mockito.times(1))
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));
  }

  @Test
  public void testAddAssertionRunEventAllFields() throws Exception, RemoteInvocationException {
    SystemEntityClient mockedEntityClient = mock(SystemEntityClient.class);
    AssertionService assertionService =
        new AssertionService(
            mockedEntityClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);
    Long eventtime = 1718619000000L;
    StringMap nativeResults = new StringMap(Map.of("prop-1", "value-1"));
    StringMap errorProps = new StringMap(Map.of("message", "errorMessage"));
    String externalUrlOfAssertion = "https://abc/xyz";

    Mockito.doAnswer(
            invocation -> {
              MetadataChangeProposal proposal = invocation.getArgument(1);

              Assert.assertEquals(proposal.getEntityUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(proposal.getAspectName(), ASSERTION_RUN_EVENT_ASPECT_NAME);
              AssertionRunEvent runEvent =
                  GenericRecordUtils.deserializeAspect(
                      proposal.getAspect().getValue(),
                      proposal.getAspect().getContentType(),
                      AssertionRunEvent.class);

              Assert.assertEquals(runEvent.getAssertionUrn(), TEST_ASSERTION_URN);
              Assert.assertEquals(runEvent.getAsserteeUrn(), TEST_DATASET_URN);
              Assert.assertEquals(runEvent.getTimestampMillis(), eventtime);
              Assert.assertEquals(runEvent.getStatus(), AssertionRunStatus.COMPLETE);
              Assert.assertEquals(runEvent.getResult().getNativeResults(), nativeResults);

              AssertionResult result = runEvent.getResult();
              Assert.assertEquals(result.getType(), AssertionResultType.ERROR);
              Assert.assertEquals(result.getExternalUrl(), externalUrlOfAssertion);
              Assert.assertEquals(
                  result.getError().getType(), AssertionResultErrorType.UNKNOWN_ERROR);
              Assert.assertEquals(result.getError().getProperties(), errorProps);

              return null;
            })
        .when(mockedEntityClient)
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));

    assertionService.addAssertionRunEvent(
        opContext,
        TEST_ASSERTION_URN,
        TEST_DATASET_URN,
        eventtime,
        new AssertionResult()
            .setType(AssertionResultType.ERROR)
            .setExternalUrl(externalUrlOfAssertion)
            .setNativeResults(nativeResults)
            .setError(
                new AssertionResultError()
                    .setType(AssertionResultErrorType.UNKNOWN_ERROR)
                    .setProperties(errorProps)));

    Mockito.verify(mockedEntityClient, Mockito.times(1))
        .ingestProposal(any(OperationContext.class), Mockito.any(), Mockito.eq(false));
  }

  @Test
  public void testUpdateDatasetAssertionRequiredFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn assertionUrn = TEST_ASSERTION_URN;
    DatasetAssertionScope scope = DatasetAssertionScope.DATASET_ROWS;
    AssertionStdOperator operator = AssertionStdOperator.CONTAIN;

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 1);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.updateDatasetAssertion(
            opContext, assertionUrn, scope, null, null, operator, null, null);

    // Assert result
    Assert.assertEquals(result, TEST_ASSERTION_URN);
  }

  @Test
  public void testUpdateDatasetAssertionAllFields() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = createMockEntityClient();
    Urn assertionUrn = TEST_ASSERTION_URN;
    DatasetAssertionScope scope = DatasetAssertionScope.DATASET_COLUMN;
    List<Urn> fields = ImmutableList.of();
    AssertionStdAggregation aggregation = AssertionStdAggregation.MAX;
    AssertionStdOperator operator = AssertionStdOperator.CONTAIN;
    AssertionStdParameters parameters =
        new AssertionStdParameters()
            .setValue(
                new AssertionStdParameter()
                    .setType(AssertionStdParameterType.NUMBER)
                    .setValue("1"));
    AssertionActions actions =
        new AssertionActions()
            .setOnSuccess(new AssertionActionArray())
            .setOnFailure(new AssertionActionArray());

    Mockito.doAnswer(
            invocation -> {
              List<MetadataChangeProposal> aspects = invocation.getArgument(1);
              Assert.assertEquals(aspects.size(), 2);
              return null;
            })
        .when(mockClient)
        .batchIngestProposals(any(OperationContext.class), Mockito.anyList(), Mockito.eq(false));

    final AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    // Test method
    Urn result =
        service.updateDatasetAssertion(
            opContext, assertionUrn, scope, fields, aggregation, operator, parameters, actions);

    // Assert result
    Assert.assertEquals(result, TEST_ASSERTION_URN);
  }

  @Test
  public void testPatchAssertionsSummary() throws Exception {
    // Test data and mocks

    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    AssertionsSummaryPatchBuilder mockPatchBuilder = mock(AssertionsSummaryPatchBuilder.class);
    Mockito.when(mockPatchBuilder.build()).thenReturn(mockAssertionSummaryMcp());

    // Test method
    service.patchAssertionsSummary(opContext, mockPatchBuilder);

    // Verify that ingestProposal was called once
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class), Mockito.eq(mockAssertionSummaryMcp()), Mockito.eq(false));
  }

  @Test
  public void testPatchAssertionRunSummary() throws Exception {
    // Test data and mocks

    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    AssertionRunSummaryPatchBuilder mockPatchBuilder = mock(AssertionRunSummaryPatchBuilder.class);
    Mockito.when(mockPatchBuilder.build()).thenReturn(mockAssertionRunSummaryMcp());

    // Test method
    service.patchAssertionRunSummary(opContext, mockPatchBuilder);

    // Verify that ingestProposal was called once
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.eq(mockAssertionRunSummaryMcp()),
            Mockito.eq(false));
  }

  @Test
  public void testListEntitiesWithAssertionInSummary() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Urn urn1 = TEST_DATASET_URN;
    Urn urn2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,test,PROD)");

    Filter expectedFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    ImmutableList.of(
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        CriterionUtils.buildCriterion(
                                            PASSING_ASSERTIONS_INDEX_FIELD_NAME,
                                            Condition.EQUAL,
                                            TEST_ASSERTION_URN.toString())))),
                        new ConjunctiveCriterion()
                            .setAnd(
                                new CriterionArray(
                                    ImmutableList.of(
                                        CriterionUtils.buildCriterion(
                                            FAILING_ASSERTIONS_INDEX_FIELD_NAME,
                                            Condition.EQUAL,
                                            TEST_ASSERTION_URN.toString())))))));

    Mockito.when(
            mockClient.searchAcrossEntities(
                any(OperationContext.class),
                Mockito.eq(ENTITY_TYPES_WITH_ASSERTION_SUMMARIES),
                Mockito.eq("*"),
                Mockito.eq(expectedFilter),
                Mockito.eq(0),
                Mockito.eq(MAX_ENTITIES_TO_LIST),
                Mockito.eq(Collections.emptyList()),
                Mockito.eq(null)))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setNumEntities(1)
                .setEntities(
                    new SearchEntityArray(
                        ImmutableList.of(
                            new SearchEntity().setEntity(urn1),
                            new SearchEntity().setEntity(urn2)))));

    AssertionService service =
        new AssertionService(
            mockClient, mock(GraphClient.class), mock(OpenApiClient.class), objectMapper);

    List<Urn> urns = service.listEntitiesWithAssertionInSummary(opContext, TEST_ASSERTION_URN);
    Assert.assertEquals(urns.size(), 2);
    Assert.assertTrue(urns.contains(urn1));
    Assert.assertTrue(urns.contains(urn2));
  }

  @Test
  public void testGetAssertionUrnsForEntity() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    Mockito.when(
            mockGraphClient.getRelatedEntities(
                Mockito.eq(TEST_DATASET_URN.toString()),
                Mockito.eq(ImmutableSet.of("Asserts")),
                Mockito.eq(RelationshipDirection.INCOMING),
                Mockito.eq(0),
                Mockito.eq(1000),
                Mockito.anyString()))
        .thenReturn(
            new EntityRelationships()
                .setTotal(3)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(
                            new EntityRelationship().setEntity(TEST_ASSERTION_URN),
                            new EntityRelationship().setEntity(TEST_FRESHNESS_ASSERTION_URN),
                            new EntityRelationship().setEntity(TEST_VOLUME_ASSERTION_URN)))));

    Mockito.when(
            mockClient.exists(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(false)))
        .thenReturn(true);

    Mockito.when(
            mockClient.exists(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_FRESHNESS_ASSERTION_URN),
                Mockito.eq(false)))
        .thenReturn(true);

    // VOLUME assertion DOES NOT EXIST! Should be filtered.
    Mockito.when(
            mockClient.exists(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_VOLUME_ASSERTION_URN),
                Mockito.eq(false)))
        .thenReturn(false);

    final AssertionService service =
        new AssertionService(mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    // Test method
    final List<Urn> assertionUrns = service.getAssertionUrnsForEntity(opContext, TEST_DATASET_URN);

    // Assert result
    Assert.assertEquals(assertionUrns.size(), 2);
    Assert.assertTrue(assertionUrns.contains(TEST_ASSERTION_URN));
    Assert.assertTrue(assertionUrns.contains(TEST_FRESHNESS_ASSERTION_URN));
    Assert.assertFalse(assertionUrns.contains(TEST_VOLUME_ASSERTION_URN));
  }

  @Test
  public void testGetEntityUrnForAssertion() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    Mockito.when(
            mockGraphClient.getRelatedEntities(
                Mockito.eq(TEST_ASSERTION_URN.toString()),
                Mockito.eq(ImmutableSet.of("Asserts")),
                Mockito.eq(RelationshipDirection.OUTGOING),
                Mockito.eq(0),
                Mockito.eq(1),
                Mockito.anyString()))
        .thenReturn(
            new EntityRelationships()
                .setTotal(1)
                .setRelationships(
                    new EntityRelationshipArray(
                        ImmutableList.of(new EntityRelationship().setEntity(TEST_DATASET_URN)))));

    final AssertionService service =
        new AssertionService(mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    // Test method
    final Urn entityUrn = service.getEntityUrnForAssertion(opContext, TEST_ASSERTION_URN);

    // Assert result
    Assert.assertEquals(entityUrn, TEST_DATASET_URN);
  }

  @Test
  public void getLatestAssertionRunEvent() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    AssertionRunEvent latestRunEvent =
        new AssertionRunEvent()
            .setResult(new AssertionResult().setType(AssertionResultType.SUCCESS));

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN.toString()),
                Mockito.eq(ASSERTION_ENTITY_NAME),
                Mockito.eq(ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null)))
        .thenReturn(
            ImmutableList.of(
                new com.linkedin.metadata.aspect.EnvelopedAspect()
                    .setAspect(GenericRecordUtils.serializeAspect(latestRunEvent))));

    final AssertionService service =
        new AssertionService(mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    // Test method
    final AssertionRunEvent actualEvent =
        service.getLatestAssertionRunEvent(opContext, TEST_ASSERTION_URN);

    // Assert results are equal
    Assert.assertEquals(actualEvent, latestRunEvent);
  }

  @Test
  public void getLatestAssertionRunEventNoEvents() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN.toString()),
                Mockito.eq(ASSERTION_ENTITY_NAME),
                Mockito.eq(ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null)))
        .thenReturn(Collections.emptyList());

    final AssertionService service =
        new AssertionService(mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    // Test method
    final AssertionRunEvent actualEvent =
        service.getLatestAssertionRunEvent(opContext, TEST_ASSERTION_URN);

    Assert.assertNull(actualEvent);
  }

  @Test
  public void getLatestAssertionRunResultNullResult() throws Exception {
    // Test data and mocks
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    GraphClient mockGraphClient = mock(GraphClient.class);

    AssertionRunEvent latestRunEvent = new AssertionRunEvent();

    Mockito.when(
            mockClient.getTimeseriesAspectValues(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_ASSERTION_URN.toString()),
                Mockito.eq(ASSERTION_ENTITY_NAME),
                Mockito.eq(ASSERTION_RUN_EVENT_ASPECT_NAME),
                Mockito.eq(null),
                Mockito.eq(null),
                Mockito.eq(1),
                Mockito.eq(null)))
        .thenReturn(
            ImmutableList.of(
                new com.linkedin.metadata.aspect.EnvelopedAspect()
                    .setAspect(GenericRecordUtils.serializeAspect(latestRunEvent))));

    final AssertionService service =
        new AssertionService(mockClient, mockGraphClient, mock(OpenApiClient.class), objectMapper);

    // Test method
    final AssertionResult actualEvent =
        service.getLatestAssertionRunResult(opContext, TEST_ASSERTION_URN);

    Assert.assertNull(actualEvent);
  }

  private static SystemEntityClient createMockEntityClient() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);

    // Init for assertion info
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(TEST_ASSERTION_URN),
                Mockito.eq(
                    ImmutableSet.of(
                        Constants.ASSERTION_INFO_ASPECT_NAME,
                        ASSERTION_ACTIONS_ASPECT_NAME,
                        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                        Constants.GLOBAL_TAGS_ASPECT_NAME,
                        ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_ASSERTION_URN)
                .setEntityName(ASSERTION_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(mockAssertionInfo().data())),
                            ASSERTION_RUN_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockAssertionRunSummary().data())),
                            DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockDataPlatformInstance().data()))))));
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(TEST_NON_EXISTENT_ASSERTION_URN),
                Mockito.eq(
                    ImmutableSet.of(
                        Constants.ASSERTION_INFO_ASPECT_NAME,
                        ASSERTION_ACTIONS_ASPECT_NAME,
                        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                        Constants.GLOBAL_TAGS_ASPECT_NAME,
                        ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_NON_EXISTENT_ASSERTION_URN)
                .setEntityName(ASSERTION_ENTITY_NAME)
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(TEST_FRESHNESS_ASSERTION_URN),
                Mockito.eq(
                    ImmutableSet.of(
                        ASSERTION_INFO_ASPECT_NAME,
                        ASSERTION_ACTIONS_ASPECT_NAME,
                        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                        Constants.GLOBAL_TAGS_ASPECT_NAME,
                        ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_ASSERTION_URN)
                .setEntityName(ASSERTION_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockFreshnessAssertionInfo().data()))))));
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(TEST_VOLUME_ASSERTION_URN),
                Mockito.eq(
                    ImmutableSet.of(
                        ASSERTION_INFO_ASPECT_NAME,
                        ASSERTION_ACTIONS_ASPECT_NAME,
                        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                        Constants.GLOBAL_TAGS_ASPECT_NAME,
                        ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_VOLUME_ASSERTION_URN)
                .setEntityName(ASSERTION_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockVolumeAssertionInfo().data()))))));
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
                Mockito.eq(TEST_SQL_ASSERTION_URN),
                Mockito.eq(
                    ImmutableSet.of(
                        ASSERTION_INFO_ASPECT_NAME,
                        ASSERTION_ACTIONS_ASPECT_NAME,
                        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                        Constants.GLOBAL_TAGS_ASPECT_NAME,
                        ASSERTION_RUN_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_SQL_ASSERTION_URN)
                .setEntityName(ASSERTION_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            ASSERTION_INFO_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockSqlAssertionInfo().data()))))));

    // Init for assertions summary
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(DATASET_ENTITY_NAME),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(ImmutableSet.of(ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_DATASET_URN)
                .setEntityName(DATASET_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            ASSERTIONS_SUMMARY_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(mockAssertionSummary().data()))))));
    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(DATASET_ENTITY_NAME),
                Mockito.eq(TEST_NON_EXISTENT_DATASET_URN),
                Mockito.eq(ImmutableSet.of(ASSERTIONS_SUMMARY_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_NON_EXISTENT_DATASET_URN)
                .setEntityName(DATASET_ENTITY_NAME)
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    // Init for update summary
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class),
                Mockito.eq(mockAssertionSummaryMcp()),
                Mockito.eq(false)))
        .thenReturn(TEST_DATASET_URN.toString());

    return mockClient;
  }

  private static AssertionInfo mockAssertionInfo() throws Exception {
    final AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.DATASET);
    info.setDatasetAssertion(new DatasetAssertionInfo().setDataset(TEST_DATASET_URN));
    return info;
  }

  private static DataPlatformInstance mockDataPlatformInstance() throws Exception {
    final DataPlatformInstance instance = new DataPlatformInstance();
    instance.setPlatform(TEST_PLATFORM_URN);
    return instance;
  }

  private static AssertionInfo mockFreshnessAssertionInfo() throws Exception {
    final AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.FRESHNESS);
    info.setFreshnessAssertion(new FreshnessAssertionInfo().setEntity(TEST_DATASET_URN));
    return info;
  }

  private static AssertionInfo mockVolumeAssertionInfo() throws Exception {
    final AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.VOLUME);
    info.setVolumeAssertion(new VolumeAssertionInfo().setEntity(TEST_DATASET_URN));
    return info;
  }

  private static AssertionInfo mockSqlAssertionInfo() throws Exception {
    final AssertionInfo info = new AssertionInfo();
    info.setType(AssertionType.SQL);
    info.setSqlAssertion(new SqlAssertionInfo().setEntity(TEST_DATASET_URN));
    return info;
  }

  private static AssertionsSummary mockAssertionSummary() throws Exception {
    final AssertionsSummary summary = new AssertionsSummary();
    summary.setFailingAssertions(new UrnArray(ImmutableList.of(TEST_ASSERTION_URN)));
    return summary;
  }

  private static AssertionRunSummary mockAssertionRunSummary() throws Exception {
    final AssertionRunSummary summary = new AssertionRunSummary();
    summary.setLastErroredAtMillis(10L);
    return summary;
  }

  private static AssertionActions mockAssertionActions() throws Exception {
    final AssertionActions actions = new AssertionActions();
    actions.setOnFailure(
        new AssertionActionArray(
            ImmutableList.of(new AssertionAction().setType(AssertionActionType.RAISE_INCIDENT))));
    actions.setOnSuccess(
        new AssertionActionArray(
            ImmutableList.of(new AssertionAction().setType(AssertionActionType.RESOLVE_INCIDENT))));
    return actions;
  }

  private static MetadataChangeProposal mockAssertionSummaryMcp() throws Exception {

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(TEST_DATASET_URN);
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setAspectName(ASSERTIONS_SUMMARY_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(mockAssertionSummary()));

    return mcp;
  }

  private static MetadataChangeProposal mockAssertionRunSummaryMcp() throws Exception {

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(TEST_ASSERTION_URN);
    mcp.setEntityType(ASSERTION_ENTITY_NAME);
    mcp.setAspectName(ASSERTION_RUN_SUMMARY_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(mockAssertionRunSummary()));

    return mcp;
  }

  private static MetadataChangeProposal mockAssertionActionsMcp() throws Exception {

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(TEST_ASSERTION_URN);
    mcp.setEntityType(ASSERTION_ENTITY_NAME);
    mcp.setAspectName(ASSERTION_ACTIONS_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(mockAssertionActions()));

    return mcp;
  }

  private static AssertionInfo mockAssertionInfoWithLastUpdated(
      AssertionInfo baseInfo, long tsMillis) {
    AssertionInfo info = baseInfo;
    info.setLastUpdated(new AuditStamp().setTime(tsMillis).setActor(TEST_ACTOR_URN));
    return info;
  }

  private static MetadataChangeProposal mockAssertionInfoLastUpdatedMcp(
      AssertionInfo baseInfo, Long tsMillis) throws Exception {

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(TEST_ASSERTION_URN);
    mcp.setEntityType(ASSERTION_ENTITY_NAME);
    mcp.setAspectName(ASSERTION_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(
        GenericRecordUtils.serializeAspect(mockAssertionInfoWithLastUpdated(baseInfo, tsMillis)));

    return mcp;
  }
}
