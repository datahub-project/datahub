package com.linkedin.gms.factory.kafka.trace;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.trace.TraceConsumerPoolExhaustedException;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.event.kafka.CheckedConsumer;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KafkaTraceConsumerPoolTest {

  private DefaultKafkaConsumerFactory<String, GenericRecord> consumerFactory;

  @BeforeMethod
  public void setup() {
    consumerFactory = mock(DefaultKafkaConsumerFactory.class);
  }

  @Test
  public void testBorrowAndReturn() throws Exception {
    KafkaConsumer<String, GenericRecord> kafkaConsumer = mock(KafkaConsumer.class);
    when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    when(consumerFactory.createConsumer()).thenReturn(kafkaConsumer);

    KafkaConsumerPool delegate =
        new KafkaConsumerPool(
            consumerFactory, 1, 2, Duration.ofSeconds(2), Duration.ofMinutes(5), null);
    KafkaTraceConsumerPool pool = new KafkaTraceConsumerPool(delegate, 1000, "mcp", null);

    String topic = "MetadataChangeProposal_v1";
    String result =
        pool.withConsumer(
            topic,
            consumer -> {
              assertEquals(consumer, kafkaConsumer);
              return "done";
            });

    assertEquals(result, "done");
    assertEquals(delegate.getTotalConsumersCreated().get(), 1);
  }

  @Test
  public void testExhaustedPoolThrows() throws InterruptedException {
    KafkaConsumer<String, GenericRecord> kafkaConsumer = mock(KafkaConsumer.class);
    when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    when(consumerFactory.createConsumer()).thenReturn(kafkaConsumer);

    KafkaConsumerPool delegate =
        new KafkaConsumerPool(
            consumerFactory, 1, 1, Duration.ofSeconds(2), Duration.ofMinutes(5), null);
    KafkaTraceConsumerPool pool = new KafkaTraceConsumerPool(delegate, 100, "mcp", null);

    CheckedConsumer checkedConsumer =
        delegate.borrowConsumer(100, TimeUnit.MILLISECONDS, "MetadataChangeProposal_v1");
    assertNotNull(checkedConsumer);

    TraceConsumerPoolExhaustedException exception =
        org.testng.Assert.expectThrows(
            TraceConsumerPoolExhaustedException.class,
            () ->
                pool.withConsumer(
                    "MetadataChangeProposal_v1",
                    consumer -> {
                      throw new AssertionError("should not run");
                    }));

    assertTrue(exception.getMessage().contains("mcp"));
    delegate.returnConsumer(checkedConsumer);
  }

  @Test
  public void testAvailableConsumerCountReflectsActiveBorrows() throws Exception {
    KafkaConsumer<String, GenericRecord> kafkaConsumer = mock(KafkaConsumer.class);
    when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    when(consumerFactory.createConsumer()).thenReturn(kafkaConsumer);

    KafkaConsumerPool delegate =
        new KafkaConsumerPool(
            consumerFactory, 1, 1, Duration.ofSeconds(2), Duration.ofMinutes(5), null);
    KafkaTraceConsumerPool pool = new KafkaTraceConsumerPool(delegate, 1000, "mcp", null);

    assertEquals(pool.getAvailableConsumerCount(), 1.0);

    CountDownLatch borrowStarted = new CountDownLatch(1);
    CountDownLatch releaseBorrow = new CountDownLatch(1);
    Thread borrower =
        new Thread(
            () ->
                pool.withConsumer(
                    "MetadataChangeProposal_v1",
                    consumer -> {
                      borrowStarted.countDown();
                      try {
                        releaseBorrow.await(5, TimeUnit.SECONDS);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                      return null;
                    }),
            "trace-pool-borrower");
    borrower.start();

    assertTrue(borrowStarted.await(5, TimeUnit.SECONDS));
    assertEquals(pool.getAvailableConsumerCount(), 0.0);

    releaseBorrow.countDown();
    borrower.join(5000);
    assertEquals(pool.getAvailableConsumerCount(), 1.0);
  }

  @Test
  public void testShutdownClosesConsumers() throws Exception {
    KafkaConsumer<String, GenericRecord> kafkaConsumer = mock(KafkaConsumer.class);
    when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    when(consumerFactory.createConsumer()).thenReturn(kafkaConsumer);

    KafkaConsumerPool delegate =
        new KafkaConsumerPool(
            consumerFactory, 1, 1, Duration.ofSeconds(2), Duration.ofMinutes(5), null);
    KafkaTraceConsumerPool pool = new KafkaTraceConsumerPool(delegate, 1000, "mcp", null);

    pool.withConsumer("MetadataChangeProposal_v1", consumer -> null);
    pool.shutdown();

    verify(kafkaConsumer).close();
    assertTrue(delegate.isShuttingDown());
    assertEquals(delegate.getTotalConsumersCreated().get(), 0);
  }

  @Test
  public void testRegisterMetricsSkipsNullRegistry() {
    KafkaConsumer<String, GenericRecord> kafkaConsumer = mock(KafkaConsumer.class);
    when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
    when(consumerFactory.createConsumer()).thenReturn(kafkaConsumer);

    MetricUtils metricUtils = mock(MetricUtils.class);
    when(metricUtils.getRegistry()).thenReturn(null);

    KafkaConsumerPool delegate =
        new KafkaConsumerPool(
            consumerFactory, 1, 1, Duration.ofSeconds(2), Duration.ofMinutes(5), null);

    org.testng.Assert.assertNotNull(new KafkaTraceConsumerPool(delegate, 1000, "mcp", metricUtils));
  }
}
