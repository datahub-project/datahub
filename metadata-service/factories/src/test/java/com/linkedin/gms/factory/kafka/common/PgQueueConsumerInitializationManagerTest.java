package com.linkedin.gms.factory.kafka.common;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.testng.annotations.Test;

public class PgQueueConsumerInitializationManagerTest {

  @Test
  public void initializeRunsRegisteredStartersOnce() {
    PgQueueConsumerInitializationManager manager = new PgQueueConsumerInitializationManager();
    AtomicBoolean ran = new AtomicBoolean(false);
    manager.whenStarted(() -> ran.set(true));

    manager.initialize("test");
    assertTrue(ran.get());
    assertTrue(manager.isInitialized());

    AtomicBoolean second = new AtomicBoolean(false);
    manager.whenStarted(() -> second.set(true));
    assertTrue(second.get());
  }

  @Test
  public void onStateChangeStartsOnAcceptingTraffic() {
    PgQueueConsumerInitializationManager manager = new PgQueueConsumerInitializationManager();
    AtomicBoolean ran = new AtomicBoolean(false);
    manager.whenStarted(() -> ran.set(true));

    manager.onStateChange(new AvailabilityChangeEvent<>(manager, ReadinessState.ACCEPTING_TRAFFIC));

    assertTrue(ran.get());
    assertTrue(manager.isInitialized());
  }

  @Test
  public void initializeIsIdempotent() {
    PgQueueConsumerInitializationManager manager = new PgQueueConsumerInitializationManager();
    AtomicBoolean ran = new AtomicBoolean(false);
    manager.whenStarted(() -> ran.set(true));

    manager.initialize("first");
    manager.initialize("second");

    assertTrue(ran.get());
    assertTrue(manager.isInitialized());
  }

  @Test
  public void notInitializedBeforeStart() {
    PgQueueConsumerInitializationManager manager = new PgQueueConsumerInitializationManager();
    assertFalse(manager.isInitialized());
  }
}
