package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.SYNC_INDEX_UPDATE_HEADER_NAME;
import static com.linkedin.metadata.Constants.UI_SOURCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityServiceImplPreprocessEventTest {

  @Mock private EbeanAspectDao mockAspectDao;
  @Mock private EventProducer mockEventProducer;
  @Mock private UpdateIndicesService mockUpdateIndicesService;
  @Mock private PreProcessHooks mockPreProcessHooks;
  @Mock private OperationContext mockOperationContext;

  private EntityServiceImpl entityService;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    entityService =
        new EntityServiceImpl(mockAspectDao, mockEventProducer, false, mockPreProcessHooks, true);
    entityService.setUpdateIndicesService(mockUpdateIndicesService);
  }

  @Test
  public void testPreprocessEventWithUISource() {
    // Create event with UI source
    MetadataChangeLog event = createTestEvent("urn:li:dataset:1");
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    event.setSystemMetadata(systemMetadata);

    when(mockPreProcessHooks.isUiEnabled()).thenReturn(true);

    boolean result = entityService.preprocessEvent(mockOperationContext, event);

    assertTrue(result);
    // Verify that handleChangeEvent was called
    verify(mockUpdateIndicesService, times(1))
        .handleChangeEvent(eq(mockOperationContext), eq(event));
  }

  @Test
  public void testPreprocessEventWithSyncIndexUpdateHeader() {
    // Create event with sync index update header
    MetadataChangeLog event = createTestEvent("urn:li:dataset:1");
    StringMap headers = new StringMap();
    headers.put(SYNC_INDEX_UPDATE_HEADER_NAME, "true");
    event.setHeaders(headers);

    boolean result = entityService.preprocessEvent(mockOperationContext, event);

    assertTrue(result);
    // Verify that handleChangeEvent was called
    verify(mockUpdateIndicesService, times(1))
        .handleChangeEvent(eq(mockOperationContext), eq(event));
  }

  @Test
  public void testPreprocessEventWithBothUISourceAndSyncHeader() {
    // Create event with both UI source and sync header
    MetadataChangeLog event = createTestEvent("urn:li:dataset:1");

    // Add UI source
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    event.setSystemMetadata(systemMetadata);

    // Add sync header
    StringMap headers = new StringMap();
    headers.put(SYNC_INDEX_UPDATE_HEADER_NAME, "true");
    event.setHeaders(headers);

    when(mockPreProcessHooks.isUiEnabled()).thenReturn(true);

    boolean result = entityService.preprocessEvent(mockOperationContext, event);

    assertTrue(result);
    // Verify that handleChangeEvent was called
    verify(mockUpdateIndicesService, times(1))
        .handleChangeEvent(eq(mockOperationContext), eq(event));
  }

  @Test
  public void testPreprocessEventWithNoPreprocessingNeeded() {
    // Create event without UI source or sync header
    MetadataChangeLog event = createTestEvent("urn:li:dataset:1");

    when(mockPreProcessHooks.isUiEnabled()).thenReturn(true);

    boolean result = entityService.preprocessEvent(mockOperationContext, event);

    assertFalse(result);
    // Verify that handleChangeEvent was not called
    verify(mockUpdateIndicesService, never())
        .handleChangeEvent(any(OperationContext.class), any(MetadataChangeLog.class));
  }

  @Test
  public void testPreprocessEventWithNullUpdateIndicesService() {
    // Set updateIndicesService to null
    entityService.setUpdateIndicesService(null);

    MetadataChangeLog event = createTestEvent("urn:li:dataset:1");
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    event.setSystemMetadata(systemMetadata);

    when(mockPreProcessHooks.isUiEnabled()).thenReturn(true);

    boolean result = entityService.preprocessEvent(mockOperationContext, event);

    assertFalse(result);
    // Verify that handleChangeEvent was not called
    verify(mockUpdateIndicesService, never())
        .handleChangeEvent(any(OperationContext.class), any(MetadataChangeLog.class));
  }

  @Test
  public void testPreprocessEventWithUISourceButPreProcessHooksDisabled() {
    // Create event with UI source but preprocess hooks disabled
    MetadataChangeLog event = createTestEvent("urn:li:dataset:1");
    SystemMetadata systemMetadata = new SystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    event.setSystemMetadata(systemMetadata);

    when(mockPreProcessHooks.isUiEnabled()).thenReturn(false);

    boolean result = entityService.preprocessEvent(mockOperationContext, event);

    assertFalse(result);
    // Verify that handleChangeEvent was not called
    verify(mockUpdateIndicesService, never())
        .handleChangeEvent(any(OperationContext.class), any(MetadataChangeLog.class));
  }

  @Test
  public void testPreprocessEventWithSyncHeaderFalse() {
    // Create event with sync header set to false
    MetadataChangeLog event = createTestEvent("urn:li:dataset:1");
    StringMap headers = new StringMap();
    headers.put(SYNC_INDEX_UPDATE_HEADER_NAME, "false");
    event.setHeaders(headers);

    boolean result = entityService.preprocessEvent(mockOperationContext, event);

    assertFalse(result);
    // Verify that handleChangeEvent was not called
    verify(mockUpdateIndicesService, never())
        .handleChangeEvent(any(OperationContext.class), any(MetadataChangeLog.class));
  }

  @Test
  public void testPreprocessEventWithSyncHeaderCaseInsensitive() {
    // Create event with sync header set to TRUE (uppercase)
    MetadataChangeLog event = createTestEvent("urn:li:dataset:1");
    StringMap headers = new StringMap();
    headers.put(SYNC_INDEX_UPDATE_HEADER_NAME, "TRUE");
    event.setHeaders(headers);

    boolean result = entityService.preprocessEvent(mockOperationContext, event);

    assertTrue(result);
    // Verify that handleChangeEvent was called
    verify(mockUpdateIndicesService, times(1))
        .handleChangeEvent(eq(mockOperationContext), eq(event));
  }

  @Test
  public void testPreprocessEventWithNullSystemMetadata() {
    // Create event with null system metadata
    MetadataChangeLog event = createTestEvent("urn:li:dataset:1");
    // Don't set systemMetadata at all - it will be null by default

    when(mockPreProcessHooks.isUiEnabled()).thenReturn(true);

    boolean result = entityService.preprocessEvent(mockOperationContext, event);

    assertFalse(result);
    // Verify that handleChangeEvent was not called
    verify(mockUpdateIndicesService, never())
        .handleChangeEvent(any(OperationContext.class), any(MetadataChangeLog.class));
  }

  @Test
  public void testPreprocessEventWithNullHeaders() {
    // Create event with null headers
    MetadataChangeLog event = createTestEvent("urn:li:dataset:1");
    // Don't set headers at all - it will be null by default

    boolean result = entityService.preprocessEvent(mockOperationContext, event);

    assertFalse(result);
    // Verify that handleChangeEvent was not called
    verify(mockUpdateIndicesService, never())
        .handleChangeEvent(any(OperationContext.class), any(MetadataChangeLog.class));
  }

  private MetadataChangeLog createTestEvent(String urnString) {
    MetadataChangeLog event = new MetadataChangeLog();
    Urn urn = UrnUtils.getUrn(urnString);
    event.setEntityUrn(urn);
    event.setEntityType("dataset");
    event.setAspectName("datasetProperties");
    event.setChangeType(ChangeType.UPSERT);
    return event;
  }
}
