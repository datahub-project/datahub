package com.linkedin.metadata.trace;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.testng.annotations.Test;

public class TraceConsumerPoolsTest {

  @Test
  public void testSingleConsumer_DoesNotCloseBetweenBorrows() {
    @SuppressWarnings("unchecked")
    Consumer<String, GenericRecord> consumer = mock(Consumer.class);
    TraceConsumerPool pool = TraceConsumerPools.singleConsumer(consumer);

    pool.withConsumer("topic", c -> "first");
    pool.withConsumer("topic", c -> "second");

    verify(consumer, times(0)).close();
    assertEquals(pool.withConsumer("topic", c -> c), consumer);
  }
}
