package com.linkedin.metadata.dao.producer;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.metadata.dao.producer.context.outbound.OutboundContextResolver;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mockito.InOrder;
import org.testng.annotations.Test;

public class KafkaUsageEventPublisherTest {
  @Test
  public void enrichesRecordBeforeSend() {
    Producer<String, String> producer = mock(Producer.class);
    KafkaHealthChecker healthChecker = mock(KafkaHealthChecker.class);
    MetricUtils metricUtils = mock(MetricUtils.class);
    OutboundContextResolver resolver = mock(OutboundContextResolver.class);
    OperationContext operationContext = mock(OperationContext.class);
    Callback callback = mock(Callback.class);
    when(healthChecker.getKafkaCallBack(eq(metricUtils), eq("USAGE"), eq("key")))
        .thenReturn(callback);
    KafkaUsageEventPublisher publisher =
        new KafkaUsageEventPublisher(producer, healthChecker, metricUtils, resolver);

    publisher.publish(operationContext, "topic", "key", "payload");

    InOrder inOrder = inOrder(resolver, producer);
    inOrder.verify(resolver).apply(any(ProducerRecord.class), eq(operationContext));
    inOrder.verify(producer).send(any(ProducerRecord.class), eq(callback));
  }
}
