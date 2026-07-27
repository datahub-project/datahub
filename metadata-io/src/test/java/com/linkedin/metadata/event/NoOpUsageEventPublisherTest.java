package com.linkedin.metadata.event;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.Future;
import org.testng.annotations.Test;

public class NoOpUsageEventPublisherTest {

  @Test
  public void publishReturnsCompletedFuture() throws Exception {
    NoOpUsageEventPublisher publisher = new NoOpUsageEventPublisher();
    publisher.setWritable(true);
    Future<?> future = publisher.publish("topic", "key", "{}");
    assertNotNull(future);
    assertTrue(future.isDone());
    publisher.flush();
  }
}
