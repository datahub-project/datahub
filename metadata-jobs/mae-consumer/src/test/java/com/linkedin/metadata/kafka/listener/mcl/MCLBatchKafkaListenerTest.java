package com.linkedin.metadata.kafka.listener.mcl;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MCLBatchKafkaListenerTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,test.dataset,PROD)";
  private static final String TEST_ASPECT_NAME = "datasetProperties";
  private static final String TEST_ENTITY_TYPE = "dataset";
  private static final String TEST_TOPIC = "MetadataChangeLog_v1";
  private static final String TEST_CONSUMER_GROUP = "mcl-batch-consumer-group";

  @Mock private MetricUtils metricUtils;

  private MCLBatchKafkaListener listener;
  private MetadataChangeLogHook mockHook1;
  private MetadataChangeLogHook mockHook2;
  private OperationContext systemOperationContext;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    when(metricUtils.getRegistry()).thenReturn(meterRegistry);

    systemOperationContext =
        TestOperationContexts.Builder.builder()
            .systemTelemetryContextSupplier(
                () ->
                    SystemTelemetryContext.builder()
                        .tracer(SystemTelemetryContext.TEST.getTracer())
                        .metricUtils(metricUtils)
                        .build())
            .buildSystemContext();

    listener = new MCLBatchKafkaListener();

    mockHook1 = spy(new TestBatchHook("hook1"));
    mockHook2 = spy(new TestBatchHook("hook2"));

    Map<String, Set<String>> aspectsToDrop = new HashMap<>();
    aspectsToDrop.put("dataset", new HashSet<>(Arrays.asList("deprecation")));
    aspectsToDrop.put("*", new HashSet<>(Arrays.asList("status")));

    listener.init(
        systemOperationContext,
        TEST_CONSUMER_GROUP,
        Arrays.asList(mockHook1, mockHook2),
        false,
        aspectsToDrop);
  }

  @Test
  public void testConsumeBatchInvokesHooksWithAllEvents() throws Exception {
    MetadataChangeLog mcl1 = createTestMCL("datasetProperties", ChangeType.UPSERT);
    MetadataChangeLog mcl2 = createTestMCL("ownership", ChangeType.UPSERT);

    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(createMockConsumerRecord(0), createMockConsumerRecord(1));

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(mcl1).thenReturn(mcl2);

      listener.consumeBatch(records);

      verify(mockHook1).invokeBatch(argThat(events -> events.size() == 2));
      verify(mockHook2).invokeBatch(argThat(events -> events.size() == 2));
    }
  }

  @Test
  public void testConsumeBatchFiltersDroppedAspects() throws Exception {
    MetadataChangeLog kept = createTestMCL("datasetProperties", ChangeType.UPSERT);
    MetadataChangeLog dropped = createTestMCL("deprecation", ChangeType.UPSERT);

    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(createMockConsumerRecord(0), createMockConsumerRecord(1));

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils
          .when(() -> EventUtils.avroToPegasusMCL(any()))
          .thenReturn(kept)
          .thenReturn(dropped);

      listener.consumeBatch(records);

      verify(mockHook1)
          .invokeBatch(
              argThat(
                  events -> {
                    List<MetadataChangeLog> list = new ArrayList<>(events);
                    return list.size() == 1
                        && "datasetProperties".equals(list.get(0).getAspectName());
                  }));
    }
  }

  @Test
  public void testConsumeBatchFiltersWildcardDroppedAspects() throws Exception {
    MetadataChangeLog kept = createTestMCL("datasetProperties", ChangeType.UPSERT);
    MetadataChangeLog dropped = createTestMCL("status", ChangeType.UPSERT);
    dropped.setEntityType("chart");

    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(createMockConsumerRecord(0), createMockConsumerRecord(1));

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils
          .when(() -> EventUtils.avroToPegasusMCL(any()))
          .thenReturn(kept)
          .thenReturn(dropped);

      listener.consumeBatch(records);

      verify(mockHook1).invokeBatch(argThat(events -> events.size() == 1));
    }
  }

  @Test
  public void testConsumeBatchHandlesDeserializationError() throws Exception {
    MetadataChangeLog validMcl = createTestMCL("datasetProperties", ChangeType.UPSERT);

    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(createMockConsumerRecord(0), createMockConsumerRecord(1));

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils
          .when(() -> EventUtils.avroToPegasusMCL(any()))
          .thenThrow(new IOException("bad record"))
          .thenReturn(validMcl);

      listener.consumeBatch(records);

      verify(mockHook1).invokeBatch(argThat(events -> events.size() == 1));
    }
  }

  @Test
  public void testConsumeBatchAllDeserializationErrorsSkipsHooks() throws Exception {
    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(createMockConsumerRecord(0), createMockConsumerRecord(1));

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils
          .when(() -> EventUtils.avroToPegasusMCL(any()))
          .thenThrow(new IOException("bad record"));

      listener.consumeBatch(records);

      verify(mockHook1, never()).invokeBatch(any());
      verify(mockHook2, never()).invokeBatch(any());
    }
  }

  @Test
  public void testConsumeBatchEmptyListSkipsHooks() throws Exception {
    listener.consumeBatch(Collections.emptyList());

    verify(mockHook1, never()).invokeBatch(any());
    verify(mockHook2, never()).invokeBatch(any());
  }

  @Test
  public void testConsumeBatchHookFailureContinuesToNextHook() throws Exception {
    MetadataChangeLog mcl = createTestMCL("datasetProperties", ChangeType.UPSERT);

    List<ConsumerRecord<String, GenericRecord>> records = List.of(createMockConsumerRecord(0));

    doThrow(new RuntimeException("hook1 failed")).when(mockHook1).invokeBatch(any());

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils.when(() -> EventUtils.avroToPegasusMCL(any())).thenReturn(mcl);

      listener.consumeBatch(records);

      verify(mockHook1).invokeBatch(any());
      verify(mockHook2).invokeBatch(any());
    }
  }

  @Test
  public void testConsumeBatchAllEventsDroppedSkipsHooks() throws Exception {
    MetadataChangeLog dropped1 = createTestMCL("deprecation", ChangeType.UPSERT);
    MetadataChangeLog dropped2 = createTestMCL("status", ChangeType.UPSERT);

    List<ConsumerRecord<String, GenericRecord>> records =
        List.of(createMockConsumerRecord(0), createMockConsumerRecord(1));

    try (MockedStatic<EventUtils> eventUtils = mockStatic(EventUtils.class)) {
      eventUtils
          .when(() -> EventUtils.avroToPegasusMCL(any()))
          .thenReturn(dropped1)
          .thenReturn(dropped2);

      listener.consumeBatch(records);

      verify(mockHook1, never()).invokeBatch(any());
      verify(mockHook2, never()).invokeBatch(any());
    }
  }

  private MetadataChangeLog createTestMCL(String aspectName, ChangeType changeType)
      throws URISyntaxException {
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(Urn.createFromString(TEST_ENTITY_URN));
    mcl.setAspectName(aspectName);
    mcl.setEntityType(TEST_ENTITY_TYPE);
    mcl.setChangeType(changeType);
    mcl.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
    return mcl;
  }

  private ConsumerRecord<String, GenericRecord> createMockConsumerRecord(int offset) {
    @SuppressWarnings("unchecked")
    ConsumerRecord<String, GenericRecord> record = mock(ConsumerRecord.class);
    when(record.topic()).thenReturn(TEST_TOPIC);
    when(record.partition()).thenReturn(0);
    when(record.offset()).thenReturn((long) offset);
    when(record.timestamp()).thenReturn(System.currentTimeMillis() - 1000);
    when(record.key()).thenReturn("key-" + offset);
    when(record.serializedValueSize()).thenReturn(512);
    when(record.value()).thenReturn(mock(GenericRecord.class));
    return record;
  }

  static class TestBatchHook implements MetadataChangeLogHook {
    private final String name;

    TestBatchHook(String name) {
      this.name = name;
    }

    @Nonnull
    @Override
    public String getConsumerGroupSuffix() {
      return "";
    }

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public void invoke(@Nonnull MetadataChangeLog event) {}
  }
}
