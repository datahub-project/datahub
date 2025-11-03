package com.linkedin.metadata.kafka.hook;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UpdateIndicesHookBatchTest {

  private UpdateIndicesService mockUpdateIndicesService;
  private OperationContext mockOperationContext;
  private UpdateIndicesHook hook;

  @BeforeEach
  public void setUp() {
    mockUpdateIndicesService = mock(UpdateIndicesService.class);
    mockOperationContext = mock(OperationContext.class);
    hook = new UpdateIndicesHook(mockUpdateIndicesService, true, true, "test-suffix");
    hook.init(mockOperationContext);
  }

  @Test
  public void testInvokeBatchWithMultipleEvents() {
    // Create test events
    MetadataChangeLog event1 = createTestEvent("urn:li:dataset:1");
    MetadataChangeLog event2 = createTestEvent("urn:li:dataset:2");
    MetadataChangeLog event3 = createTestEvent("urn:li:dataset:3");

    List<MetadataChangeLog> events = Arrays.asList(event1, event2, event3);

    // Invoke batch processing
    hook.invokeBatch(events);

    // Verify that handleChangeEvents was called once with all events
    verify(mockUpdateIndicesService, times(1))
        .handleChangeEvents(eq(mockOperationContext), eq(events));
  }

  @Test
  public void testInvokeBatchWithEmptyCollection() {
    // Invoke batch processing with empty collection
    hook.invokeBatch(Collections.emptyList());

    // Verify that handleChangeEvents was not called
    verify(mockUpdateIndicesService, times(0))
        .handleChangeEvents(any(OperationContext.class), any(Collection.class));
  }

  @Test
  public void testInvokeBatchFiltersUIEvents() {
    // Create events with UI source (should be filtered out when reprocessUIEvents is false)
    MetadataChangeLog uiEvent = createTestEventWithUI("urn:li:dataset:1");
    MetadataChangeLog normalEvent = createTestEvent("urn:li:dataset:2");

    // Create hook with reprocessUIEvents = false
    UpdateIndicesHook hookNoReprocess =
        new UpdateIndicesHook(mockUpdateIndicesService, true, false, "test-suffix");
    hookNoReprocess.init(mockOperationContext);

    List<MetadataChangeLog> events = Arrays.asList(uiEvent, normalEvent);

    // Invoke batch processing
    hookNoReprocess.invokeBatch(events);

    // Verify that handleChangeEvents was called only with the non-UI event
    verify(mockUpdateIndicesService, times(1))
        .handleChangeEvents(eq(mockOperationContext), eq(Arrays.asList(normalEvent)));
  }

  private MetadataChangeLog createTestEvent(String urnString) {
    MetadataChangeLog event = new MetadataChangeLog();
    Urn urn = UrnUtils.getUrn(urnString);
    event.setEntityUrn(urn);
    event.setEntityType("dataset");
    event.setAspectName("datasetProperties");
    event.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);
    return event;
  }

  private MetadataChangeLog createTestEventWithUI(String urnString) {
    MetadataChangeLog event = createTestEvent(urnString);

    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put("app", "ui");
    systemMetadata.setProperties(properties);
    event.setSystemMetadata(systemMetadata);

    return event;
  }
}
