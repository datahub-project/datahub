package com.linkedin.metadata.boot;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for GracefulShutdownHandler.
 *
 * <p>Note: This handler is only registered as a Spring bean when server.shutdown=graceful
 * (via @ConditionalOnProperty). In tests, we instantiate it directly to verify its behavior.
 */
public class GracefulShutdownHandlerTest {

  private GracefulShutdownHandler handler;

  @BeforeMethod
  public void setUp() {
    handler = new GracefulShutdownHandler();
  }

  @Test
  public void testShutdownInProgressInitiallyFalse() {
    // When: Handler is created
    // Then: Shutdown should not be in progress
    assertFalse(handler.isShutdownInProgress());
  }

  @Test
  public void testContextClosedEventTriggersShutdown() {
    // When: Application context closes (SIGTERM)
    // Create a minimal mock to avoid NPE if implementation changes to use the event
    ContextClosedEvent mockEvent = new ContextClosedEvent(mock(ApplicationContext.class));
    handler.onApplicationClosed(mockEvent);

    // Then: Shutdown flag should be set
    assertTrue(handler.isShutdownInProgress());
  }
}
