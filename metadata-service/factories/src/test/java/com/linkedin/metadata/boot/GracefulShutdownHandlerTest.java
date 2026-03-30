package com.linkedin.metadata.boot;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for GracefulShutdownHandler, verifying that Kafka listeners are properly shut down
 * when the application context closes (SIGTERM received).
 */
@ExtendWith(MockitoExtension.class)
class GracefulShutdownHandlerTest {

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
    // Note: Handler ignores the event argument, so we pass null to make intent clear
    handler.onApplicationClosed(null);

    // Then: Shutdown flag should be set
    assertTrue(handler.isShutdownInProgress());
  }
}
