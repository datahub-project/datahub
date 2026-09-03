package com.linkedin.metadata.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeEvent;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetadataChangeEventsProcessorTest {

  @Mock private SystemEntityClient entityClient;
  @Mock private Producer kafkaProducer;
  @Mock private EventProducer kafkaEventProducer;
  @Mock private MetricUtils metricUtils;

  private OperationContext systemOperationContext;
  private MetadataChangeEventsProcessor processor;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    when(metricUtils.getRegistry()).thenReturn(new SimpleMeterRegistry());

    systemOperationContext =
        TestOperationContexts.Builder.builder()
            .systemTelemetryContextSupplier(
                () ->
                    SystemTelemetryContext.builder()
                        .tracer(SystemTelemetryContext.TEST.getTracer())
                        .metricUtils(metricUtils)
                        .build())
            .buildSystemContext();

    processor =
        new MetadataChangeEventsProcessor(
            systemOperationContext, entityClient, kafkaProducer, kafkaEventProducer);
    ReflectionTestUtils.setField(processor, "fmceTopicName", "FailedMetadataChangeEvent_v4");
    ReflectionTestUtils.setField(processor, "mceConsumerGroupId", "mce-consumer-job-client");
  }

  @Test
  public void testConsumeKafkaSuccessful() throws Exception {
    GenericRecord mockGenericRecord = mock(GenericRecord.class);
    MetadataChangeEvent mockMce = mock(MetadataChangeEvent.class);
    when(mockMce.hasProposedSnapshot()).thenReturn(false);

    ConsumerRecord<String, GenericRecord> consumerRecord = mock(ConsumerRecord.class);
    when(consumerRecord.value()).thenReturn(mockGenericRecord);
    when(consumerRecord.key()).thenReturn("test-key");
    when(consumerRecord.topic()).thenReturn("MetadataChangeEvent_v4");
    when(consumerRecord.partition()).thenReturn(0);
    when(consumerRecord.offset()).thenReturn(42L);
    when(consumerRecord.timestamp()).thenReturn(System.currentTimeMillis());
    when(consumerRecord.serializedValueSize()).thenReturn(1024);

    try (MockedStatic<EventUtils> mockedEventUtils = Mockito.mockStatic(EventUtils.class)) {
      mockedEventUtils
          .when(() -> EventUtils.avroToPegasusMCE(mockGenericRecord))
          .thenReturn(mockMce);

      processor.consume(consumerRecord);

      mockedEventUtils.verify(() -> EventUtils.avroToPegasusMCE(mockGenericRecord));
    }
  }
}
