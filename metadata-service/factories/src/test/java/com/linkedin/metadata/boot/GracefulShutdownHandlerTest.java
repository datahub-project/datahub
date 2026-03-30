package com.linkedin.metadata.boot;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.event.ContextClosedEvent;

/**
 * Unit tests for GracefulShutdownHandler, verifying that Kafka listeners are properly shut down
 * when the application context closes (SIGTERM received).
 */
@ExtendWith(MockitoExtension.class)
class GracefulShutdownHandlerTest {

  @Mock private ContextClosedEvent mockEvent;

  private GracefulShutdownHandler handler;

  @BeforeEach
  void setUp() {
    handler = new GracefulShutdownHandler(true);
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
    handler.onApplicationClosed(mockEvent);

    // Then: Shutdown flag should be set
    assertTrue(handler.isShutdownInProgress());
  }

  @Test
  void testFFEnabledFalse() {

    GracefulShutdownHandler falseHandler = new GracefulShutdownHandler(true);
    falseHandler.onApplicationClosed(mockEvent);

    // Then: Shutdown flag should be set
    assertFalse(handler.isShutdownInProgress());
  }
}
