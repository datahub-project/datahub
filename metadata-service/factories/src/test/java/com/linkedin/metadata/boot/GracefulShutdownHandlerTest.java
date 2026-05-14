package com.linkedin.metadata.boot;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for GracefulShutdownHandler.
 *
 * <p>Note: This handler is only registered as a Spring bean when server.shutdown=graceful
 * (via @ConditionalOnProperty). In tests, we instantiate it directly to verify its behavior.
 */
public class GracefulShutdownHandlerTest {

  private GracefulShutdownHandler handler;

  @BeforeEach
  void setUp() {
    handler = new GracefulShutdownHandler();
  }

  @Test
  void testShutdownInProgressInitiallyFalse() {
    // When: Handler is created
    // Then: Shutdown should not be in progress
    assertFalse(handler.isShutdownInProgress());
  }

  @Test
  void testContextClosedEventTriggersShutdown() {
    // When: Application context closes (SIGTERM)
    // Create a minimal mock to avoid NPE if implementation changes to use the event
    org.springframework.context.event.ContextClosedEvent mockEvent =
        new org.springframework.context.event.ContextClosedEvent(
            org.mockito.Mockito.mock(org.springframework.context.ApplicationContext.class));
    handler.onApplicationClosed(mockEvent);

    // Then: Shutdown flag should be set
    assertTrue(handler.isShutdownInProgress());
  }
}
