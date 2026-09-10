package com.linkedin.metadata.kafka.hook.assertion;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

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
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AssertionRunSummaryHookTest {

  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");

  private AssertionService assertionService;
  private OperationContext operationContext;
  private MetadataChangeLog event;
  private AssertionRunSummaryHook hook;

  @BeforeMethod
  public void setup() {
    assertionService = mock(AssertionService.class);
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    event = mock(MetadataChangeLog.class);
    hook = new AssertionRunSummaryHook(assertionService, true);
    hook.init(operationContext);

    when(event.getEntityType()).thenReturn(Constants.ASSERTION_ENTITY_NAME);
    when(event.getEntityUrn()).thenReturn(TEST_ASSERTION_URN);
    when(event.hasEntityUrn()).thenReturn(true);
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(event.getAspectName()).thenReturn(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME);
  }

  @DataProvider
  public Object[][] resultTypes() {
    return new Object[][] {
      {AssertionResultType.SUCCESS, AssertionStatus.PASSING},
      {AssertionResultType.FAILURE, AssertionStatus.FAILING},
      {AssertionResultType.ERROR, AssertionStatus.ERROR},
      {AssertionResultType.INIT, AssertionStatus.INIT}
    };
  }

  @Test(dataProvider = "resultTypes")
  public void testProcessesCompletedRun(
      AssertionResultType resultType, AssertionStatus expectedStatus) throws Exception {
    when(event.getAspect())
        .thenReturn(GenericRecordUtils.serializeAspect(runEvent(resultType, 1000L)));

    hook.invoke(operationContext, event);

    verify(assertionService)
        .patchAssertionRunSummary(
            eq(operationContext), eq(expectedPatch(resultType, expectedStatus, 1000L)));
  }

  @Test(dataProvider = "resultTypes")
  public void testDoesNotReplaceNewerSummary(
      AssertionResultType resultType, AssertionStatus ignoredStatus) throws Exception {
    when(assertionService.getAssertionRunSummary(operationContext, TEST_ASSERTION_URN))
        .thenReturn(summaryWithTimestamp(resultType, 2000L));
    when(event.getAspect())
        .thenReturn(GenericRecordUtils.serializeAspect(runEvent(resultType, 1000L)));

    hook.invoke(operationContext, event);

    verify(assertionService, never())
        .patchAssertionRunSummary(
            any(OperationContext.class), any(AssertionRunSummaryPatchBuilder.class));
  }

  @Test
  public void testSkipsIneligibleEvents() throws Exception {
    when(event.getEntityType()).thenReturn(Constants.DATASET_ENTITY_NAME);
    hook.invoke(operationContext, event);

    when(event.getEntityType()).thenReturn(Constants.ASSERTION_ENTITY_NAME);
    when(event.getChangeType()).thenReturn(ChangeType.DELETE);
    hook.invoke(operationContext, event);

    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(event.getAspectName()).thenReturn(Constants.ASSERTION_INFO_ASPECT_NAME);
    hook.invoke(operationContext, event);

    when(event.getAspectName()).thenReturn(Constants.ASSERTION_RUN_EVENT_ASPECT_NAME);
    when(event.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(
                new AssertionRunEvent()
                    .setTimestampMillis(1000L)
                    .setRunId("run")
                    .setAssertionUrn(TEST_ASSERTION_URN)
                    .setAsserteeUrn(UrnUtils.getUrn("urn:li:dataset:test"))
                    .setStatus(AssertionRunStatus.COMPLETE)));
    hook.invoke(operationContext, event);

    verify(assertionService, never())
        .patchAssertionRunSummary(
            any(OperationContext.class), any(AssertionRunSummaryPatchBuilder.class));
  }

  @Test
  public void testDisabledHookDoesNotProcessEvents() throws Exception {
    AssertionRunSummaryHook disabledHook = new AssertionRunSummaryHook(assertionService, false);
    when(event.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent(AssertionResultType.SUCCESS, 1000L)));

    disabledHook.invoke(operationContext, event);

    assertFalse(disabledHook.isEnabled());
    verify(assertionService, never())
        .patchAssertionRunSummary(
            any(OperationContext.class), any(AssertionRunSummaryPatchBuilder.class));
  }

  @Test
  public void testPatchFailureDoesNotFailHook() throws Exception {
    when(event.getAspect())
        .thenReturn(
            GenericRecordUtils.serializeAspect(runEvent(AssertionResultType.SUCCESS, 1000L)));
    doThrow(new RuntimeException("patch failed"))
        .when(assertionService)
        .patchAssertionRunSummary(
            any(OperationContext.class), any(AssertionRunSummaryPatchBuilder.class));

    hook.invoke(operationContext, event);

    verify(assertionService)
        .patchAssertionRunSummary(
            any(OperationContext.class), any(AssertionRunSummaryPatchBuilder.class));
  }

  @Test
  public void testConfigurationAndInitialization() {
    AssertionRunSummaryHook configuredHook =
        new AssertionRunSummaryHook(assertionService, true, "-summary");

    assertTrue(configuredHook.isEnabled());
    assertEquals(configuredHook.getConsumerGroupSuffix(), "-summary");
    assertSame(configuredHook.init(operationContext), configuredHook);
  }

  private AssertionRunEvent runEvent(AssertionResultType resultType, long timestamp) {
    return new AssertionRunEvent()
        .setTimestampMillis(timestamp)
        .setRunId("run")
        .setAssertionUrn(TEST_ASSERTION_URN)
        .setAsserteeUrn(UrnUtils.getUrn("urn:li:dataset:test"))
        .setStatus(AssertionRunStatus.COMPLETE)
        .setResult(new AssertionResult().setType(resultType));
  }

  private AssertionRunSummary summaryWithTimestamp(AssertionResultType resultType, long timestamp) {
    AssertionRunSummary summary = new AssertionRunSummary();
    switch (resultType) {
      case SUCCESS:
        return summary.setLastPassedAtMillis(timestamp);
      case FAILURE:
        return summary.setLastFailedAtMillis(timestamp);
      case ERROR:
        return summary.setLastErroredAtMillis(timestamp);
      case INIT:
        return summary.setLastInitializedAtMillis(timestamp);
      default:
        throw new IllegalArgumentException("Unsupported result type " + resultType);
    }
  }

  private AssertionRunSummaryPatchBuilder expectedPatch(
      AssertionResultType resultType, AssertionStatus status, long timestamp) {
    AssertionRunSummaryPatchBuilder builder =
        new AssertionRunSummaryPatchBuilder().setAssertionStatus(status.name());
    builder.urn(TEST_ASSERTION_URN);
    switch (resultType) {
      case SUCCESS:
        return builder.setLastPassedAt(timestamp);
      case FAILURE:
        return builder.setLastFailedAt(timestamp);
      case ERROR:
        return builder.setLastErroredAt(timestamp);
      case INIT:
        return builder.setLastInitializedAt(timestamp);
      default:
        throw new IllegalArgumentException("Unsupported result type " + resultType);
    }
  }
}
