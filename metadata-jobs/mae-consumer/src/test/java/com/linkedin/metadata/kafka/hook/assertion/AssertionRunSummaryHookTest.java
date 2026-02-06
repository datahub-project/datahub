package com.linkedin.metadata.kafka.hook.assertion;

import static org.mockito.Mockito.*;

import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.assertion.AssertionStatus;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.builder.AssertionRunSummaryPatchBuilder;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.MonitorService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorError;
import com.linkedin.monitor.MonitorErrorType;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AssertionRunSummaryHookTest {

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");

  @Mock private AssertionService assertionService;
  @Mock private MonitorService monitorService;

  @Mock private OperationContext systemOperationContext;

  @Mock private MetadataChangeLog mockMetadataChangeLog;

  private AssertionRunSummaryHook assertionRunSummaryHook;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    assertionRunSummaryHook = new AssertionRunSummaryHook(assertionService, monitorService, true);
    assertionRunSummaryHook.init(systemOperationContext);
    when(mockMetadataChangeLog.getEntityType()).thenReturn(Constants.ASSERTION_ENTITY_NAME);
    when(mockMetadataChangeLog.getEntityUrn()).thenReturn(TEST_ASSERTION_URN);
    when(mockMetadataChangeLog.hasEntityUrn()).thenReturn(true);
    when(mockMetadataChangeLog.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockMetadataChangeLog.getAspectName())
        .thenReturn(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME);
    when(assertionService.getMonitorUrnForAssertion(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);
  }

  @Test
  public void testHandleAssertionRunSuccessNoExistingSummary() throws Exception {
    // Arrange
    AssertionRunEvent runEvent =
        createMockAssertionRunEvent(
            AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS, 1000L);
    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null); // no existing summary
    when(assertionService.getMonitorUrnForAssertion(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    AssertionRunSummaryPatchBuilder expectedPatchBuilder =
        new AssertionRunSummaryPatchBuilder()
            .setLastPassedAt(1000L)
            .setAssertionStatus(AssertionStatus.PASSING.name());

    // Assert
    verify(assertionService, times(1))
        .patchAssertionRunSummary(any(OperationContext.class), eq(expectedPatchBuilder));
  }

  @Test
  public void testHandleAssertionRunSuccessExistingSummary() throws Exception {
    // Arrange
    AssertionRunEvent runEvent =
        createMockAssertionRunEvent(
            AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS, 1000L);

    // Case 1: Existing Summary at lower timestamp

    AssertionRunSummary existingSummary = new AssertionRunSummary();
    existingSummary.setLastPassedAtMillis(500L);

    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(existingSummary); //
    when(assertionService.getMonitorUrnForAssertion(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    AssertionRunSummaryPatchBuilder expectedPatchBuilder =
        new AssertionRunSummaryPatchBuilder()
            .setLastPassedAt(1000L)
            .setAssertionStatus(AssertionStatus.PASSING.name());

    // Assert
    verify(assertionService, times(1))
        .patchAssertionRunSummary(any(OperationContext.class), eq(expectedPatchBuilder));

    // Case 2: Existing Summary at higher timestamp
    existingSummary.setLastPassedAtMillis(1500L);

    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(existingSummary); //
    when(assertionService.getMonitorUrnForAssertion(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    // Assert only 1 update (not 2)
    verify(assertionService, times(1))
        .patchAssertionRunSummary(
            any(OperationContext.class), any(AssertionRunSummaryPatchBuilder.class));
  }

  @Test
  public void testHandleAssertionRunSuccessMonitorErrorOverridesStatus() throws Exception {
    AssertionRunEvent runEvent =
        createMockAssertionRunEvent(
            AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS, 1000L);
    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);

    Urn monitorUrn = UrnUtils.getUrn("urn:li:monitor:(urn:li:dataset:test,test)");
    when(assertionService.getMonitorUrnForAssertion(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(monitorUrn);

    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    monitorInfo.setStatus(
        new MonitorStatus()
            .setError(new MonitorError().setType(MonitorErrorType.INPUT_DATA_INVALID)));
    when(monitorService.getMonitorInfo(systemOperationContext, monitorUrn)).thenReturn(monitorInfo);

    when(mockMetadataChangeLog.getAspect())
        .thenReturn(GenericRecordUtils.serializeAspect(runEvent));

    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    AssertionRunSummaryPatchBuilder expectedPatchBuilder =
        new AssertionRunSummaryPatchBuilder()
            .setLastPassedAt(1000L)
            .setAssertionStatus(AssertionStatus.ERROR.name());

    verify(assertionService, times(1))
        .patchAssertionRunSummary(any(OperationContext.class), eq(expectedPatchBuilder));
  }

  @Test
  public void testHandleAssertionRunFailureNoExistingSummary() throws Exception {
    // Arrange
    AssertionRunEvent runEvent =
        createMockAssertionRunEvent(
            AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE, 2000L);
    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);
    when(assertionService.getMonitorUrnForAssertion(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    AssertionRunSummaryPatchBuilder expectedPatchBuilder =
        new AssertionRunSummaryPatchBuilder()
            .setLastFailedAt(2000L)
            .setAssertionStatus(AssertionStatus.FAILING.name());

    // Assert
    verify(assertionService, times(1))
        .patchAssertionRunSummary(any(OperationContext.class), eq(expectedPatchBuilder));
  }

  @Test
  public void testHandleAssertionRunFailureExistingSummary() throws Exception {
    // Arrange
    AssertionRunEvent runEvent =
        createMockAssertionRunEvent(
            AssertionRunStatus.COMPLETE, AssertionResultType.FAILURE, 1000L);

    // Case 1: Existing Summary at lower timestamp

    AssertionRunSummary existingSummary = new AssertionRunSummary();
    existingSummary.setLastFailedAtMillis(500L);

    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(existingSummary); //
    when(assertionService.getMonitorUrnForAssertion(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    AssertionRunSummaryPatchBuilder expectedPatchBuilder =
        new AssertionRunSummaryPatchBuilder()
            .setLastFailedAt(1000L)
            .setAssertionStatus(AssertionStatus.FAILING.name());

    // Assert
    verify(assertionService, times(1))
        .patchAssertionRunSummary(any(OperationContext.class), eq(expectedPatchBuilder));

    // Case 2: Existing Summary at higher timestamp
    existingSummary.setLastFailedAtMillis(1500L);

    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(existingSummary); //
    when(assertionService.getMonitorUrnForAssertion(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    // Assert only 1 update (not 2)
    verify(assertionService, times(1))
        .patchAssertionRunSummary(
            any(OperationContext.class), any(AssertionRunSummaryPatchBuilder.class));
  }

  @Test
  public void testHandleAssertionRunErrorNoExistingSummary() throws Exception {
    // Arrange
    AssertionRunEvent runEvent =
        createMockAssertionRunEvent(AssertionRunStatus.COMPLETE, AssertionResultType.ERROR, 3000L);
    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    AssertionRunSummaryPatchBuilder expectedPatchBuilder =
        new AssertionRunSummaryPatchBuilder()
            .setLastErroredAt(3000L)
            .setAssertionStatus(AssertionStatus.ERROR.name());

    // Assert
    verify(assertionService, times(1))
        .patchAssertionRunSummary(any(OperationContext.class), eq(expectedPatchBuilder));
  }

  @Test
  public void testHandleAssertionRunErrorExistingSummary() throws Exception {
    // Arrange
    AssertionRunEvent runEvent =
        createMockAssertionRunEvent(AssertionRunStatus.COMPLETE, AssertionResultType.ERROR, 1000L);

    // Case 1: Existing Summary at lower timestamp

    AssertionRunSummary existingSummary = new AssertionRunSummary();
    existingSummary.setLastErroredAtMillis(500L);

    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(existingSummary); //
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    AssertionRunSummaryPatchBuilder expectedPatchBuilder =
        new AssertionRunSummaryPatchBuilder()
            .setLastErroredAt(1000L)
            .setAssertionStatus(AssertionStatus.ERROR.name());

    // Assert
    verify(assertionService, times(1))
        .patchAssertionRunSummary(any(OperationContext.class), eq(expectedPatchBuilder));

    // Case 2: Existing Summary at higher timestamp
    existingSummary.setLastErroredAtMillis(1500L);

    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(existingSummary); //
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    // Assert only 1 update (not 2)
    verify(assertionService, times(1))
        .patchAssertionRunSummary(
            any(OperationContext.class), any(AssertionRunSummaryPatchBuilder.class));
  }

  @Test
  public void testHandleAssertionRunInitNoExistingSummary() throws Exception {
    // Arrange
    AssertionRunEvent runEvent =
        createMockAssertionRunEvent(AssertionRunStatus.COMPLETE, AssertionResultType.INIT, 4000L);
    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(null);
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    AssertionRunSummaryPatchBuilder expectedPatchBuilder =
        new AssertionRunSummaryPatchBuilder()
            .setLastInitializedAt(4000L)
            .setAssertionStatus(AssertionStatus.INIT.name());

    // Assert
    verify(assertionService, times(1))
        .patchAssertionRunSummary(any(OperationContext.class), eq(expectedPatchBuilder));
  }

  @Test
  public void testHandleAssertionRunInitExistingSummary() throws Exception {
    // Arrange
    AssertionRunEvent runEvent =
        createMockAssertionRunEvent(AssertionRunStatus.COMPLETE, AssertionResultType.INIT, 1000L);

    // Case 1: Existing Summary at lower timestamp

    AssertionRunSummary existingSummary = new AssertionRunSummary();
    existingSummary.setLastInitializedAtMillis(500L);

    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(existingSummary); //
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    AssertionRunSummaryPatchBuilder expectedPatchBuilder =
        new AssertionRunSummaryPatchBuilder()
            .setLastInitializedAt(1000L)
            .setAssertionStatus(AssertionStatus.INIT.name());

    // Assert
    verify(assertionService, times(1))
        .patchAssertionRunSummary(any(OperationContext.class), eq(expectedPatchBuilder));

    // Case 2: Existing Summary at higher timestamp
    existingSummary.setLastInitializedAtMillis(1500L);

    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(existingSummary); //
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent)); // Mock to return the correct aspect

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    // Assert only 1 update (not 2)
    verify(assertionService, times(1))
        .patchAssertionRunSummary(
            any(OperationContext.class), any(AssertionRunSummaryPatchBuilder.class));
  }

  @Test
  public void testSkipNonEligibleEvent() throws Exception {
    // Arrange
    when(mockMetadataChangeLog.getEntityType()).thenReturn("NonAssertionEntity");

    // Act
    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    // Assert
    verify(assertionService, never())
        .patchAssertionRunSummary(
            any(OperationContext.class), any(AssertionRunSummaryPatchBuilder.class));
  }

  @Test
  public void testMonitorErrorUpdatesAssertionStatus() throws Exception {
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    monitorInfo.setStatus(
        new MonitorStatus()
            .setError(new MonitorError().setType(MonitorErrorType.INPUT_DATA_INSUFFICIENT))
            .setReviewedAt(2000L));
    monitorInfo.setAssertionMonitor(
        new AssertionMonitor().setAssertions(new AssertionEvaluationSpecArray()));
    AssertionEvaluationSpec spec = new AssertionEvaluationSpec();
    spec.setAssertion(TEST_ASSERTION_URN);
    monitorInfo.getAssertionMonitor().getAssertions().add(spec);

    when(mockMetadataChangeLog.getEntityType()).thenReturn(Constants.MONITOR_ENTITY_NAME);
    when(mockMetadataChangeLog.getAspectName()).thenReturn(Constants.MONITOR_INFO_ASPECT_NAME);
    when(mockMetadataChangeLog.getAspect())
        .thenReturn(GenericRecordUtils.serializeAspect(monitorInfo));

    when(assertionService.getAssertionRunSummary(systemOperationContext, TEST_ASSERTION_URN))
        .thenReturn(new AssertionRunSummary());

    assertionRunSummaryHook.invoke(mockMetadataChangeLog);

    AssertionRunSummaryPatchBuilder expectedPatchBuilder =
        new AssertionRunSummaryPatchBuilder().setAssertionStatus(AssertionStatus.ERROR.name());

    verify(assertionService, times(1))
        .patchAssertionRunSummary(any(OperationContext.class), eq(expectedPatchBuilder));
  }

  private AssertionRunEvent createMockAssertionRunEvent(
      AssertionRunStatus status, AssertionResultType resultType, long timestamp) {
    AssertionResult result = new AssertionResult();
    result.setType(resultType);

    AssertionRunEvent runEvent = new AssertionRunEvent();
    runEvent.setStatus(status);
    runEvent.setResult(result);
    runEvent.setTimestampMillis(timestamp);

    return runEvent;
  }
}
