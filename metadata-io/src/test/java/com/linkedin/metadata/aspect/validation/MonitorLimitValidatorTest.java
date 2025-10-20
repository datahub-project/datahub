package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.MONITOR_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.search.ScrollResult;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class MonitorLimitValidatorTest {

  @Test
  public void testValidatorDisabled() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(false).setMaxMonitors(5);

    BatchItem mockItem = createMockBatchItem();
    RetrieverContext mockContext = mock(RetrieverContext.class);

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(mockItem), mockContext);

    assertEquals(0, result.count());
  }

  @Test
  public void testNonMonitorEntity() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(true).setMaxMonitors(5);

    BatchItem mockItem = mock(BatchItem.class);
    Urn nonMonitorUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
    when(mockItem.getUrn()).thenReturn(nonMonitorUrn);
    when(mockItem.getAspectName()).thenReturn("datasetProperties");
    when(mockItem.getChangeType()).thenReturn(ChangeType.CREATE);

    RetrieverContext mockContext = mock(RetrieverContext.class);

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(mockItem), mockContext);

    assertEquals(0, result.count());
  }

  @Test
  public void testWithinLimit() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(true).setMaxMonitors(10);

    BatchItem mockItem = createMockBatchItem();
    RetrieverContext mockContext = createMockRetrieverContext(5); // 5 existing monitors

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(mockItem), mockContext);

    assertEquals(0, result.count());
  }

  @Test
  public void testExceedsLimit() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(true).setMaxMonitors(5);

    BatchItem mockItem = createMockBatchItem();
    RetrieverContext mockContext =
        createMockRetrieverContextWithExistence(
            5,
            Map.of(
                mockItem.getUrn(),
                false)); // 5 existing monitors, limit is 5, new monitor doesn't exist

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(mockItem), mockContext);

    List<AspectValidationException> exceptions = result.collect(Collectors.toList());
    assertEquals(1, exceptions.size());
    assertTrue(exceptions.get(0).getMessage().contains("Monitor limit exceeded"));
  }

  @Test
  public void testMultipleNewMonitors() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(true).setMaxMonitors(5);

    // Create two different monitor URNs
    Urn urn1 = UrnUtils.getUrn("urn:li:monitor:test-monitor-1");
    Urn urn2 = UrnUtils.getUrn("urn:li:monitor:test-monitor-2");
    BatchItem mockItem1 = createMockBatchItemWithUrn(urn1, ChangeType.CREATE);
    BatchItem mockItem2 = createMockBatchItemWithUrn(urn2, ChangeType.CREATE);

    RetrieverContext mockContext =
        createMockRetrieverContextWithExistence(
            4, Map.of(urn1, false, urn2, false)); // 4 existing monitors, both new ones don't exist

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(mockItem1, mockItem2), mockContext);

    List<AspectValidationException> exceptions = result.collect(Collectors.toList());
    assertEquals(2, exceptions.size()); // Both items should fail validation since 4 + 2 > 5
  }

  @Test
  public void testErrorHandling() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(true).setMaxMonitors(5);

    BatchItem mockItem = createMockBatchItem();
    RetrieverContext mockContext = mock(RetrieverContext.class);
    SearchRetriever mockSearchRetriever = mock(SearchRetriever.class);

    when(mockContext.getSearchRetriever()).thenReturn(mockSearchRetriever);
    when(mockSearchRetriever.scroll(anyList(), any(), anyString(), anyInt()))
        .thenThrow(new RuntimeException("Database error"));

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(mockItem), mockContext);

    // Should not throw exception, should return empty stream on error
    assertEquals(0, result.count());
  }

  @Test
  public void testUpsertExistingMonitor() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(true).setMaxMonitors(5);

    BatchItem mockItem = createMockBatchItem(ChangeType.UPSERT);
    RetrieverContext mockContext =
        createMockRetrieverContextWithExistence(
            4, Map.of(mockItem.getUrn(), true)); // Monitor already exists

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(mockItem), mockContext);

    // Should not fail validation since we're updating an existing monitor
    assertEquals(0, result.count());
  }

  @Test
  public void testUpsertNewMonitor() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(true).setMaxMonitors(5);

    BatchItem mockItem = createMockBatchItem(ChangeType.UPSERT);
    RetrieverContext mockContext =
        createMockRetrieverContextWithExistence(
            5, Map.of(mockItem.getUrn(), false)); // Monitor doesn't exist, limit is 5

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(mockItem), mockContext);

    // Should fail validation since we're at the limit and trying to create a new monitor
    List<AspectValidationException> exceptions = result.collect(Collectors.toList());
    assertEquals(1, exceptions.size());
    assertTrue(exceptions.get(0).getMessage().contains("Monitor limit exceeded"));
  }

  @Test
  public void testDuplicateUrnsInBatch() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(true).setMaxMonitors(5);

    // Create two items with the same URN
    Urn sameUrn = UrnUtils.getUrn("urn:li:monitor:duplicate-monitor");
    BatchItem mockItem1 = createMockBatchItemWithUrn(sameUrn, ChangeType.CREATE);
    BatchItem mockItem2 = createMockBatchItemWithUrn(sameUrn, ChangeType.CREATE);

    RetrieverContext mockContext =
        createMockRetrieverContextWithExistence(4, Map.of(sameUrn, false)); // Monitor doesn't exist

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(mockItem1, mockItem2), mockContext);

    // Should not fail validation since it's the same monitor being created (counted only once)
    assertEquals(0, result.count());
  }

  @Test
  public void testMixedUpsertAndCreate() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(true).setMaxMonitors(5);

    Urn existingUrn = UrnUtils.getUrn("urn:li:monitor:existing-monitor");
    Urn newUrn = UrnUtils.getUrn("urn:li:monitor:new-monitor");

    BatchItem upsertExisting = createMockBatchItemWithUrn(existingUrn, ChangeType.UPSERT);
    BatchItem createNew = createMockBatchItemWithUrn(newUrn, ChangeType.CREATE);

    RetrieverContext mockContext =
        createMockRetrieverContextWithExistence(
            4, Map.of(existingUrn, true, newUrn, false)); // One exists, one doesn't

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(upsertExisting, createNew), mockContext);

    // Should not fail validation since we're only creating 1 new monitor (4 + 1 = 5, which is at
    // the limit)
    assertEquals(0, result.count());
  }

  @Test
  public void testUpsertExistingMonitorWhenOverLimit() {
    MonitorLimitValidator validator =
        new MonitorLimitValidator().setEnabled(true).setMaxMonitors(5);

    BatchItem mockItem = createMockBatchItem(ChangeType.UPSERT);
    RetrieverContext mockContext =
        createMockRetrieverContextWithExistence(
            6,
            Map.of(
                mockItem.getUrn(), true)); // 6 existing monitors (over limit), but monitor exists

    Stream<AspectValidationException> result =
        validator.validateProposedAspects(List.of(mockItem), mockContext);

    // Should not fail validation since we're updating an existing monitor, even though we're over
    // the limit
    assertEquals(0, result.count());
  }

  private BatchItem createMockBatchItem() {
    return createMockBatchItem(ChangeType.CREATE);
  }

  private BatchItem createMockBatchItem(ChangeType changeType) {
    BatchItem mockItem = mock(BatchItem.class);
    Urn monitorUrn = UrnUtils.getUrn("urn:li:monitor:test-monitor");
    when(mockItem.getUrn()).thenReturn(monitorUrn);
    when(mockItem.getAspectName()).thenReturn(MONITOR_INFO_ASPECT_NAME);
    when(mockItem.getChangeType()).thenReturn(changeType);
    return mockItem;
  }

  private BatchItem createMockBatchItemWithUrn(Urn urn, ChangeType changeType) {
    BatchItem mockItem = mock(BatchItem.class);
    when(mockItem.getUrn()).thenReturn(urn);
    when(mockItem.getAspectName()).thenReturn(MONITOR_INFO_ASPECT_NAME);
    when(mockItem.getChangeType()).thenReturn(changeType);
    return mockItem;
  }

  private RetrieverContext createMockRetrieverContext(int existingMonitorCount) {
    return createMockRetrieverContextWithExistence(existingMonitorCount, Map.of());
  }

  private RetrieverContext createMockRetrieverContextWithExistence(
      int existingMonitorCount, Map<Urn, Boolean> existenceMap) {
    RetrieverContext mockContext = mock(RetrieverContext.class);
    SearchRetriever mockSearchRetriever = mock(SearchRetriever.class);
    ScrollResult mockScrollResult = mock(ScrollResult.class);
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);

    when(mockContext.getSearchRetriever()).thenReturn(mockSearchRetriever);
    when(mockSearchRetriever.scroll(anyList(), any(), any(), anyInt()))
        .thenReturn(mockScrollResult);
    when(mockScrollResult.getNumEntities()).thenReturn(existingMonitorCount);

    when(mockContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.entityExists(any())).thenReturn(existenceMap);

    return mockContext;
  }
}
