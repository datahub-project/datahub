package com.linkedin.metadata.trace;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.testng.annotations.Test;

public class EphemeralTraceConsumerPoolTest {

  @Test
  public void testWithConsumer_BorrowsAndReturnsConsumer() throws Exception {
    Consumer<String, GenericRecord> consumer = mock(Consumer.class);
    EphemeralTraceConsumerPool pool = new EphemeralTraceConsumerPool(() -> consumer);

    String result =
        pool.withConsumer(
            "test-topic",
            borrowed -> {
              assertEquals(borrowed, consumer);
              return "ok";
            });

    assertEquals(result, "ok");
    verify(consumer, times(1)).close();
  }

  @Test
  public void testWithConsumer_ClosesConsumerOnException() {
    Consumer<String, GenericRecord> consumer = mock(Consumer.class);
    EphemeralTraceConsumerPool pool = new EphemeralTraceConsumerPool(() -> consumer);

    assertThrows(
        RuntimeException.class,
        () ->
            pool.withConsumer(
                "test-topic",
                borrowed -> {
                  throw new RuntimeException("boom");
                }));

    verify(consumer, times(1)).close();
  }
}
