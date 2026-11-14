package com.linkedin.metadata.dao.producer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.dao.producer.context.outbound.OutboundContextResolver;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.TopicConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class KafkaEventProducerPolicyChangeTest {

  private static final Urn POLICY = UrnUtils.getUrn("urn:li:dataHubPolicy:test");

  private static MetadataChangeLog policyChangeLog() {
    return new MetadataChangeLog()
        .setEntityType("dataHubPolicy")
        .setEntityUrn(POLICY)
        .setChangeType(ChangeType.UPSERT)
        .setAspectName("dataHubPolicyInfo");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSendsTheChangeLogToThePolicyTopicKeyedByUrn() {
    Producer<String, GenericRecord> producer = mock(Producer.class);
    OutboundContextResolver resolver = mock(OutboundContextResolver.class);
    KafkaEventProducer eventProducer =
        new KafkaEventProducer(
            producer,
            new TopicConventionImpl(),
            mock(KafkaHealthChecker.class),
            mock(MetricUtils.class),
            resolver);

    eventProducer.producePolicyChange(
        mock(OperationContext.class), POLICY, policyChangeLog(), "PolicyChange_v1");

    ArgumentCaptor<ProducerRecord<String, GenericRecord>> sent =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(producer).send(sent.capture(), any(Callback.class));
    assertEquals(sent.getValue().topic(), "PolicyChange_v1");
    assertEquals(sent.getValue().key(), POLICY.toString());
    verify(resolver).apply(any(ProducerRecord.class), any(OperationContext.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReadOnlyProducerDoesNotSend() {
    Producer<String, GenericRecord> producer = mock(Producer.class);
    KafkaEventProducer eventProducer =
        new KafkaEventProducer(
            producer,
            new TopicConventionImpl(),
            mock(KafkaHealthChecker.class),
            mock(MetricUtils.class),
            mock(OutboundContextResolver.class));
    eventProducer.setWritable(false);

    eventProducer.producePolicyChange(
        mock(OperationContext.class), POLICY, policyChangeLog(), "PolicyChange_v1");

    verify(producer, never()).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testChangeLogPathAlsoSendsToThePolicyTopic() {
    Producer<String, GenericRecord> producer = mock(Producer.class);
    KafkaEventProducer eventProducer =
        new KafkaEventProducer(
            producer,
            new TopicConventionImpl(),
            mock(KafkaHealthChecker.class),
            mock(MetricUtils.class),
            mock(OutboundContextResolver.class));
    eventProducer.setPolicyChangeTopicName("PolicyChange_v1");

    eventProducer.produceMetadataChangeLog(
        mock(OperationContext.class), POLICY, mock(AspectSpec.class), policyChangeLog());

    ArgumentCaptor<ProducerRecord<String, GenericRecord>> sent =
        ArgumentCaptor.forClass(ProducerRecord.class);
    // The mocked health checker returns a null callback for the change-log send.
    verify(producer, times(2)).send(sent.capture(), any());
    List<String> topics =
        sent.getAllValues().stream().map(ProducerRecord::topic).collect(Collectors.toList());
    assertEquals(
        topics,
        List.of(
            new TopicConventionImpl().getMetadataChangeLogVersionedTopicName(), "PolicyChange_v1"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSendResultIsCounted() {
    Producer<String, GenericRecord> producer = mock(Producer.class);
    MetricUtils metrics = mock(MetricUtils.class);
    KafkaEventProducer eventProducer =
        new KafkaEventProducer(
            producer,
            new TopicConventionImpl(),
            mock(KafkaHealthChecker.class),
            metrics,
            mock(OutboundContextResolver.class));

    eventProducer.producePolicyChange(
        mock(OperationContext.class), POLICY, policyChangeLog(), "PolicyChange_v1");
    ArgumentCaptor<Callback> callback = ArgumentCaptor.forClass(Callback.class);
    verify(producer).send(any(ProducerRecord.class), callback.capture());

    callback.getValue().onCompletion(null, null);
    verify(metrics).increment(KafkaEventProducer.class, "policyChange.produced", 1);
    callback.getValue().onCompletion(null, new RuntimeException("broker unavailable"));
    verify(metrics).increment(KafkaEventProducer.class, "policyChange.produce_failed", 1);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConversionFailureIsCountedAndNotSent() {
    Producer<String, GenericRecord> producer = mock(Producer.class);
    MetricUtils metrics = mock(MetricUtils.class);
    KafkaEventProducer eventProducer =
        new KafkaEventProducer(
            producer,
            new TopicConventionImpl(),
            mock(KafkaHealthChecker.class),
            metrics,
            mock(OutboundContextResolver.class));

    try (org.mockito.MockedStatic<com.linkedin.metadata.EventUtils> utils =
        org.mockito.Mockito.mockStatic(com.linkedin.metadata.EventUtils.class)) {
      utils
          .when(() -> com.linkedin.metadata.EventUtils.pegasusToAvroMCL(any()))
          .thenThrow(new java.io.IOException("cannot convert"));

      eventProducer.producePolicyChange(
          mock(OperationContext.class), POLICY, policyChangeLog(), "PolicyChange_v1");
    }

    verify(producer, never()).send(any(), any());
    verify(metrics).increment(KafkaEventProducer.class, "policyChange.produce_failed", 1);
  }
}
