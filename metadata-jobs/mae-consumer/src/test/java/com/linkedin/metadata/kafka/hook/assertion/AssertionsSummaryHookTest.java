package com.linkedin.metadata.kafka.hook.assertion;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FixedIntervalSchedule;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionSchedule;
import com.linkedin.assertion.FreshnessAssertionScheduleType;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.builder.AssertionsSummaryPatchBuilder;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.timeseries.CalendarInterval;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AssertionsSummaryHookTest {
  private static final Urn TEST_EXISTING_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:existing-test");
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");

  private static final Urn TEST_DATASET_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name-2,PROD)");

  private static final Urn TEST_MONITOR_URN =
      UrnUtils.getUrn(String.format("urn:li:monitor:(%s,test)", TEST_DATASET_URN));

  private static final String TEST_ASSERTION_TYPE = AssertionType.DATASET.toString();
  private static final Urn TEST_INFERRED_ASSERTION_URN =
      UrnUtils.getUrn("urn:li:assertion:inferred-test");

  private OperationContext opContext;

  @BeforeTest
  public void setup() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
  }

  @Test
  public void testInvokeNotEnabled() throws Exception {
    AssertionService service = mockAssertionService(new AssertionsSummary());
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, false).init(opContext);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            mockAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(0))
        .getAssertionInfo(any(OperationContext.class), Mockito.any());
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    AssertionService service = mockAssertionService(new AssertionsSummary());
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);

    // Case 1: Incorrect aspect
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_INFO_ASPECT_NAME, ChangeType.UPSERT, new AssertionInfo());
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(0))
        .getAssertionInfo(any(OperationContext.class), Mockito.any());

    // Case 2: Run Event But Delete
    event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.DELETE,
            mockAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(0))
        .getAssertionInfo(any(OperationContext.class), Mockito.any());

    // Case 3: Monitor status change, but enabled
    event =
        buildMetadataChangeLog(
            TEST_MONITOR_URN,
            MONITOR_INFO_ASPECT_NAME,
            ChangeType.UPSERT,
            mockMonitorInfoEvent(MonitorMode.ACTIVE, ImmutableList.of(TEST_ASSERTION_URN)));
    hook.invoke(event);
    Mockito.verify(service, Mockito.times(0))
        .getAssertionInfo(any(OperationContext.class), Mockito.any());
  }

  @DataProvider(name = "assertionsSummaryProvider")
  static Object[][] assertionsSummaryProvider() {
    return new Object[][] {
      new Object[] {null},
      new Object[] {
        new AssertionsSummary()
            .setPassingAssertionDetails(new AssertionSummaryDetailsArray())
            .setFailingAssertionDetails(new AssertionSummaryDetailsArray())
      },
      new Object[] {
        new AssertionsSummary()
            .setPassingAssertionDetails(
                new AssertionSummaryDetailsArray(
                    ImmutableList.of(
                        new AssertionSummaryDetails()
                            .setUrn(TEST_EXISTING_ASSERTION_URN)
                            .setType(TEST_ASSERTION_TYPE)
                            .setLastResultAt(0L)
                            .setSource(AssertionSourceType.EXTERNAL.toString()))))
            .setFailingAssertionDetails(new AssertionSummaryDetailsArray())
      },
      new Object[] {
        new AssertionsSummary()
            .setPassingAssertionDetails(new AssertionSummaryDetailsArray())
            .setFailingAssertionDetails(
                new AssertionSummaryDetailsArray(
                    ImmutableList.of(
                        new AssertionSummaryDetails()
                            .setUrn(TEST_EXISTING_ASSERTION_URN)
                            .setType(TEST_ASSERTION_TYPE)
                            .setLastResultAt(0L)
                            .setSource(AssertionSourceType.EXTERNAL.toString()))))
      },
      new Object[] {
        new AssertionsSummary()
            .setPassingAssertionDetails(
                new AssertionSummaryDetailsArray(
                    ImmutableList.of(
                        new AssertionSummaryDetails()
                            .setUrn(TEST_EXISTING_ASSERTION_URN)
                            .setType(TEST_ASSERTION_TYPE)
                            .setLastResultAt(0L)
                            .setSource(AssertionSourceType.EXTERNAL.toString()))))
            .setFailingAssertionDetails(
                new AssertionSummaryDetailsArray(
                    ImmutableList.of(
                        new AssertionSummaryDetails()
                            .setUrn(TEST_EXISTING_ASSERTION_URN)
                            .setType(TEST_ASSERTION_TYPE)
                            .setLastResultAt(0L)
                            .setSource(AssertionSourceType.EXTERNAL.toString()))))
      }
    };
  }

  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeAssertionRunEventSuccess(AssertionsSummary initialSummary)
      throws Exception {
    AssertionService service = mockAssertionService(initialSummary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);

    final AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));

    final AssertionRunEvent runEvent =
        mockAssertionRunEvent(
            TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS);

    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, runEvent);

    hook.invoke(event);

    Mockito.verify(service, Mockito.times(1))
        .getAssertionInfo(any(OperationContext.class), eq(TEST_ASSERTION_URN));

    Mockito.verify(service, Mockito.times(1))
        .getAssertionsSummary(any(OperationContext.class), eq(TEST_DATASET_URN));

    AssertionsSummaryPatchBuilder patchBuilder = new AssertionsSummaryPatchBuilder();
    patchBuilder.urn(TEST_ASSERTION_URN);
    patchBuilder.withEntityName(TEST_DATASET_URN.getEntityType());
    patchBuilder.removeFromFailingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromErroringAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.addPassingAssertionDetails(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN, info, runEvent));
    patchBuilder.addOverallLastAssertionResultAt(runEvent.getTimestampMillis());

    // Ensure we ingested a new aspect.
    Mockito.verify(service, Mockito.times(1))
        .patchAssertionsSummary(any(OperationContext.class), eq(patchBuilder));
  }

  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeAssertionRunEventFailure(AssertionsSummary initialSummary)
      throws Exception {
    AssertionService service = mockAssertionService(initialSummary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);

    final AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));
    final AssertionRunEvent runEvent =
        mockAssertionRunEvent(
            TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, runEvent);

    hook.invoke(event);

    Mockito.verify(service, Mockito.times(1))
        .getAssertionInfo(any(OperationContext.class), eq(TEST_ASSERTION_URN));

    Mockito.verify(service, Mockito.times(1))
        .getAssertionsSummary(any(OperationContext.class), eq(TEST_DATASET_URN));

    AssertionsSummaryPatchBuilder patchBuilder = new AssertionsSummaryPatchBuilder();
    patchBuilder.urn(TEST_ASSERTION_URN);
    patchBuilder.withEntityName(TEST_DATASET_URN.getEntityType());
    patchBuilder.removeFromPassingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromErroringAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.addFailingAssertionDetails(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN, info, runEvent));
    patchBuilder.addOverallLastAssertionResultAt(runEvent.getTimestampMillis());

    // Ensure we ingested a new aspect.
    Mockito.verify(service, Mockito.times(1))
        .patchAssertionsSummary(any(OperationContext.class), eq(patchBuilder));
  }

  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeAssertionRunEventError(AssertionsSummary initialSummary) throws Exception {
    AssertionService service = mockAssertionService(initialSummary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);

    final AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));
    final AssertionRunEvent runEvent =
        mockAssertionRunEvent(
            TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.ERROR);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, runEvent);

    hook.invoke(event);

    Mockito.verify(service, Mockito.times(1))
        .getAssertionInfo(any(OperationContext.class), eq(TEST_ASSERTION_URN));

    Mockito.verify(service, Mockito.times(1))
        .getAssertionsSummary(any(OperationContext.class), eq(TEST_DATASET_URN));

    AssertionsSummaryPatchBuilder patchBuilder = new AssertionsSummaryPatchBuilder();
    patchBuilder.urn(TEST_ASSERTION_URN);
    patchBuilder.withEntityName(TEST_DATASET_URN.getEntityType());
    patchBuilder.removeFromPassingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromFailingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.addErroringAssertionDetails(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN, info, runEvent));
    patchBuilder.addOverallLastAssertionResultAt(runEvent.getTimestampMillis());

    // Ensure we ingested a new aspect.
    Mockito.verify(service, Mockito.times(1))
        .patchAssertionsSummary(any(OperationContext.class), eq(patchBuilder));
  }

  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeAssertionRunEventInit(AssertionsSummary initialSummary) throws Exception {
    AssertionService service = mockAssertionService(initialSummary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);

    final AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));
    final AssertionRunEvent runEvent =
        mockAssertionRunEvent(
            TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.INIT);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, runEvent);

    hook.invoke(event);

    Mockito.verify(service, Mockito.times(1))
        .getAssertionInfo(any(OperationContext.class), eq(TEST_ASSERTION_URN));

    Mockito.verify(service, Mockito.times(1))
        .getAssertionsSummary(any(OperationContext.class), eq(TEST_DATASET_URN));

    AssertionsSummaryPatchBuilder patchBuilder = new AssertionsSummaryPatchBuilder();
    patchBuilder.urn(TEST_ASSERTION_URN);
    patchBuilder.withEntityName(TEST_DATASET_URN.getEntityType());
    patchBuilder.removeFromPassingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromFailingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromErroringAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.addInitializingAssertionDetails(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN, info, runEvent));
    patchBuilder.addOverallLastAssertionResultAt(runEvent.getTimestampMillis());

    // Ensure we ingested a new aspect.
    Mockito.verify(service, Mockito.times(1))
        .patchAssertionsSummary(any(OperationContext.class), eq(patchBuilder));
  }

  @Test
  public void testInvokeAssertionRunEventSuccessIgnoredIfEarlier() throws Exception {

    Urn assertionUrn1 = TEST_ASSERTION_URN;
    Urn assertionUrn2 = TEST_INFERRED_ASSERTION_URN;

    AssertionsSummary existingSummary = new AssertionsSummary();
    existingSummary.setPassingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(assertionUrn1)
                    .setType(TEST_ASSERTION_TYPE)
                    .setLastResultAt(0L) // Latest result!
                    .setSource(AssertionSourceType.EXTERNAL.toString()))));
    existingSummary.setFailingAssertionDetails(
        new AssertionSummaryDetailsArray(
            ImmutableList.of(
                new AssertionSummaryDetails()
                    .setUrn(assertionUrn2)
                    .setType(TEST_ASSERTION_TYPE)
                    .setLastResultAt(0L)
                    .setSource(AssertionSourceType.EXTERNAL.toString()))));

    AssertionService service = mockAssertionService(existingSummary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);

    // 1. Mock failing assertion
    final AssertionRunEvent runEvent =
        mockAssertionRunEventWithTSMillis(
            assertionUrn1, AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE, -1L);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            assertionUrn1, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, runEvent);

    // Nothing should change.
    hook.invoke(event);

    // 2. Mock succeeding assertion that's older (i.e. was back-filled)
    final AssertionRunEvent runEvent2 =
        mockAssertionRunEventWithTSMillis(
            assertionUrn2, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS, -1L);
    final MetadataChangeLog event2 =
        buildMetadataChangeLog(
            assertionUrn2, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, runEvent2);

    // Nothing should change.
    hook.invoke(event2);

    // No interactions on summary aspects that have existing data because the timestamps are always
    // 0L (newer)
    Mockito.verify(service, Mockito.never())
        .patchAssertionsSummary(
            Mockito.any(OperationContext.class), Mockito.any(AssertionsSummaryPatchBuilder.class));
  }

  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeAssertionSoftDeleted(AssertionsSummary summary) throws Exception {
    AssertionService service = mockAssertionService(summary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, STATUS_ASPECT_NAME, ChangeType.UPSERT, mockAssertionSoftDeleted());
    hook.invoke(event);

    AssertionsSummaryPatchBuilder patchBuilder = new AssertionsSummaryPatchBuilder();
    patchBuilder.urn(TEST_ASSERTION_URN);
    patchBuilder.withEntityName(TEST_DATASET_URN.getEntityType());
    patchBuilder.removeFromPassingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromFailingAssertionDetails(TEST_ASSERTION_URN);

    // Ensure we patched correctly.
    Mockito.verify(service, Mockito.times(1))
        .patchAssertionsSummary(any(OperationContext.class), eq(patchBuilder));
  }

  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeAssertionInfoHardDeleted(AssertionsSummary summary) throws Exception {
    AssertionService service = mockAssertionService(summary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_INFO_ASPECT_NAME,
            ChangeType.DELETE,
            null,
            mockFreshnessAssertion(TEST_DATASET_URN));
    hook.invoke(event);

    AssertionsSummaryPatchBuilder patchBuilder = new AssertionsSummaryPatchBuilder();
    patchBuilder.urn(TEST_ASSERTION_URN);
    patchBuilder.withEntityName(TEST_DATASET_URN.getEntityType());
    patchBuilder.removeFromPassingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromFailingAssertionDetails(TEST_ASSERTION_URN);

    // Ensure we patched correctly.
    Mockito.verify(service, Mockito.times(1))
        .patchAssertionsSummary(any(OperationContext.class), eq(patchBuilder));
  }

  @Test
  public void testInvokeAssertionKeyHardDeleted() throws Exception {
    AssertionService service = Mockito.mock(AssertionService.class);

    Mockito.when(
            service.listEntitiesWithAssertionInSummary(
                any(OperationContext.class), eq(TEST_ASSERTION_URN)))
        .thenReturn(ImmutableList.of(TEST_DATASET_URN));

    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_KEY_ASPECT_NAME, ChangeType.DELETE, null, null);
    hook.invoke(event);

    Mockito.verify(service, Mockito.times(1))
        .listEntitiesWithAssertionInSummary(any(OperationContext.class), eq(TEST_ASSERTION_URN));

    AssertionsSummaryPatchBuilder patchBuilder = new AssertionsSummaryPatchBuilder();
    patchBuilder.urn(TEST_ASSERTION_URN);
    patchBuilder.withEntityName(TEST_DATASET_URN.getEntityType());
    patchBuilder.removeFromPassingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromFailingAssertionDetails(TEST_ASSERTION_URN);

    // Ensure we patched correctly.
    Mockito.verify(service, Mockito.times(1))
        .patchAssertionsSummary(any(OperationContext.class), eq(patchBuilder));
  }

  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeAssertionEntityUpdate(AssertionsSummary summary) throws Exception {

    // Ensure that the previous summary is removed.
    AssertionService service = mockAssertionService(summary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_INFO_ASPECT_NAME,
            ChangeType.UPSERT,
            mockFreshnessAssertion(TEST_DATASET_URN_2),
            mockFreshnessAssertion(TEST_DATASET_URN));

    hook.invoke(event);

    AssertionsSummaryPatchBuilder patchBuilder = new AssertionsSummaryPatchBuilder();
    patchBuilder.urn(TEST_ASSERTION_URN);
    patchBuilder.withEntityName(TEST_DATASET_URN.getEntityType());
    patchBuilder.removeFromPassingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromFailingAssertionDetails(TEST_ASSERTION_URN);

    // Ensure we patched correctly.
    Mockito.verify(service, Mockito.times(1))
        .patchAssertionsSummary(any(OperationContext.class), eq(patchBuilder));
  }

  @Test(dataProvider = "assertionsSummaryProvider")
  public void testInvokeMonitorDisabled(AssertionsSummary summary) throws Exception {
    AssertionService service = mockAssertionService(summary);
    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);
    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_MONITOR_URN,
            MONITOR_INFO_ASPECT_NAME,
            ChangeType.UPSERT,
            mockMonitorInfoEvent(MonitorMode.INACTIVE, ImmutableList.of(TEST_ASSERTION_URN)));
    hook.invoke(event);

    AssertionsSummaryPatchBuilder patchBuilder = new AssertionsSummaryPatchBuilder();
    patchBuilder.urn(TEST_ASSERTION_URN);
    patchBuilder.withEntityName(TEST_DATASET_URN.getEntityType());
    patchBuilder.removeFromPassingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromFailingAssertionDetails(TEST_ASSERTION_URN);

    // Ensure we patched correctly.
    Mockito.verify(service, Mockito.times(1))
        .patchAssertionsSummary(any(OperationContext.class), eq(patchBuilder));
  }

  @Test
  public void testAddAssertionToSummaryWithNullSummary() throws Exception {
    // Test the case where batchGetAssertionsSummary returns null, causing addAssertionToSummary
    // to be called with null summary parameter, which then fetches it via getAssertionsSummary
    AssertionService service = Mockito.mock(AssertionService.class);

    final AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL))
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(TEST_DATASET_URN));

    final AssertionRunEvent runEvent =
        mockAssertionRunEvent(
            TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS);

    // Mock getAssertionInfo
    Mockito.when(service.getAssertionInfo(any(OperationContext.class), eq(TEST_ASSERTION_URN)))
        .thenReturn(info);

    // Mock batchGetAssertionsSummary to return null for the entity (entity has no summary yet)
    Map<Urn, AssertionsSummary> summariesMap = new HashMap<>();
    summariesMap.put(TEST_DATASET_URN, null);
    Mockito.when(
            service.batchGetAssertionsSummary(
                any(OperationContext.class), eq(ImmutableSet.of(TEST_DATASET_URN))))
        .thenReturn(summariesMap);

    // Mock getAssertionsSummary to return null (will be called when summary parameter is null)
    Mockito.when(service.getAssertionsSummary(any(OperationContext.class), eq(TEST_DATASET_URN)))
        .thenReturn(null);

    AssertionsSummaryHook hook = new AssertionsSummaryHook(service, true).init(opContext);

    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_RUN_EVENT_ASPECT_NAME, ChangeType.UPSERT, runEvent);

    hook.invoke(event);

    // Verify batchGetAssertionsSummary was called (returns null)
    Mockito.verify(service, Mockito.times(1))
        .batchGetAssertionsSummary(
            any(OperationContext.class), eq(ImmutableSet.of(TEST_DATASET_URN)));

    // Verify getAssertionsSummary was called when summary is null
    Mockito.verify(service, Mockito.times(1))
        .getAssertionsSummary(any(OperationContext.class), eq(TEST_DATASET_URN));

    // Verify patch was created correctly
    AssertionsSummaryPatchBuilder patchBuilder = new AssertionsSummaryPatchBuilder();
    patchBuilder.urn(TEST_ASSERTION_URN);
    patchBuilder.withEntityName(TEST_DATASET_URN.getEntityType());
    patchBuilder.removeFromFailingAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.removeFromErroringAssertionDetails(TEST_ASSERTION_URN);
    patchBuilder.addPassingAssertionDetails(
        buildAssertionSummaryDetails(TEST_ASSERTION_URN, info, runEvent));
    patchBuilder.addOverallLastAssertionResultAt(runEvent.getTimestampMillis());

    Mockito.verify(service, Mockito.times(1))
        .patchAssertionsSummary(any(OperationContext.class), eq(patchBuilder));
  }

  private AssertionRunEvent mockAssertionRunEvent(
      final Urn urn, final AssertionRunStatus status, final AssertionResultType resultType) {
    return mockAssertionRunEventWithTSMillis(urn, status, resultType, 1L);
  }

  private AssertionRunEvent mockAssertionRunEventWithTSMillis(
      final Urn urn,
      final AssertionRunStatus status,
      final AssertionResultType resultType,
      long tsMillis) {
    AssertionRunEvent event = new AssertionRunEvent();
    event.setTimestampMillis(tsMillis);
    event.setAssertionUrn(urn);
    event.setStatus(status);
    event.setResult(new AssertionResult().setType(resultType).setRowCount(0L));
    return event;
  }

  private Status mockAssertionSoftDeleted() {
    Status status = new Status();
    status.setRemoved(true);
    return status;
  }

  private AssertionInfo mockFreshnessAssertion(final Urn entityUrn) {
    AssertionInfo testInfo = new AssertionInfo();
    testInfo.setType(AssertionType.FRESHNESS);
    testInfo.setFreshnessAssertion(
        new FreshnessAssertionInfo()
            .setEntity(entityUrn)
            .setType(FreshnessAssertionType.DATASET_CHANGE)
            .setSchedule(
                new FreshnessAssertionSchedule()
                    .setType(FreshnessAssertionScheduleType.FIXED_INTERVAL)
                    .setFixedInterval(
                        new FixedIntervalSchedule()
                            .setMultiple(2)
                            .setUnit(CalendarInterval.HOUR))));
    return testInfo;
  }

  private MonitorInfo mockMonitorInfoEvent(MonitorMode mode, List<Urn> assertionUrns) {
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    monitorInfo.setStatus(new MonitorStatus().setMode(mode));
    monitorInfo.setAssertionMonitor(
        new AssertionMonitor().setAssertions(new AssertionEvaluationSpecArray()));
    assertionUrns.forEach(
        urn -> {
          AssertionEvaluationSpec newSpec = new AssertionEvaluationSpec();
          newSpec.setAssertion(urn);
          monitorInfo.getAssertionMonitor().getAssertions().add(newSpec);
        });
    return monitorInfo;
  }

  private AssertionService mockAssertionService(AssertionsSummary summary) {
    AssertionService mockService = Mockito.mock(AssertionService.class);

    AssertionInfo testAssertionInfo =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL))
            .setDatasetAssertion(new DatasetAssertionInfo().setDataset(TEST_DATASET_URN));

    Mockito.when(mockService.getAssertionInfo(any(OperationContext.class), eq(TEST_ASSERTION_URN)))
        .thenReturn(testAssertionInfo);

    // Mock batchGetAssertionInfo for handleMonitorDisabled (used when monitor is disabled)
    Mockito.when(
            mockService.batchGetAssertionInfo(
                any(OperationContext.class), eq(ImmutableSet.of(TEST_ASSERTION_URN))))
        .thenReturn(ImmutableMap.of(TEST_ASSERTION_URN, testAssertionInfo));

    Mockito.when(
            mockService.getAssertionInfo(
                any(OperationContext.class), eq(TEST_INFERRED_ASSERTION_URN)))
        .thenReturn(
            new AssertionInfo()
                .setType(AssertionType.DATASET)
                .setSource(new AssertionSource().setType(AssertionSourceType.INFERRED))
                .setDatasetAssertion(new DatasetAssertionInfo().setDataset(TEST_DATASET_URN)));

    Mockito.when(
            mockService.getAssertionsSummary(any(OperationContext.class), eq(TEST_DATASET_URN)))
        .thenReturn(summary);

    return mockService;
  }

  @Nonnull
  private AssertionSummaryDetails buildAssertionSummaryDetails(
      @Nonnull final Urn urn,
      @Nonnull final AssertionInfo info,
      @Nonnull final AssertionRunEvent event) {
    AssertionSummaryDetails assertionSummaryDetails = new AssertionSummaryDetails();
    assertionSummaryDetails.setUrn(urn);
    assertionSummaryDetails.setType(info.getType().toString());
    assertionSummaryDetails.setLastResultAt(event.getTimestampMillis());
    if (info.hasSource()) {
      assertionSummaryDetails.setSource(info.getSource().getType().toString());
    }
    return assertionSummaryDetails;
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn, String aspectName, ChangeType changeType, RecordTemplate aspect) {
    return buildMetadataChangeLog(urn, aspectName, changeType, aspect, null);
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn,
      String aspectName,
      ChangeType changeType,
      RecordTemplate aspect,
      RecordTemplate prevAspect) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(urn.getEntityType());
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    if (aspect != null) {
      event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    }
    if (prevAspect != null) {
      event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevAspect));
    }
    return event;
  }
}
