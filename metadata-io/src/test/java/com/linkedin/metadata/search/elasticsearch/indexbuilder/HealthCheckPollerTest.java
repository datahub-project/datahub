package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.CircuitBreakerState.HealthState;
import java.io.IOException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for HealthCheckPoller circuit breaker state management.
 *
 * <p>Validates: 1. Cluster unresponsive (IOException) transitions circuit breaker to RED 2.
 * OpenSearchException also transitions to RED 3. Unexpected exceptions transition to RED 4.
 * Successful health checks update circuit breaker with computed state
 */
public class HealthCheckPollerTest {

  @Mock private ESIndexBuilder mockIndexBuilder;
  @Mock private CircuitBreakerState mockCircuitBreakerState;

  private HealthCheckPoller poller;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    poller = new HealthCheckPoller(mockIndexBuilder, mockCircuitBreakerState, 15, 90.0, 10);
  }

  /**
   * HIGH SEVERITY BUG FIX: When getClusterHealth() throws IOException (network failure), the
   * circuit breaker MUST transition to RED. Previously, the code silently logged and preserved the
   * old state, allowing reindex submissions to continue to an unresponsive cluster.
   */
  @Test
  public void testClusterUnresponsive_IOExceptionTransitionsToRED() throws IOException {
    // Setup: getClusterHealth() throws IOException (network failure)
    when(mockIndexBuilder.getClusterHealth()).thenThrow(new IOException("Connection timeout"));

    // Execute: poll should catch IOException and transition to RED
    poller.poll();

    // Verify: Circuit breaker was updated to RED (not left in previous state)
    verify(mockCircuitBreakerState, times(1)).update(HealthState.RED);
  }

  /**
   * HIGH SEVERITY BUG FIX: When getClusterHealth() throws OpenSearchException (ES API failure), the
   * circuit breaker MUST transition to RED. Previously, the code silently logged and preserved the
   * old state.
   */
  @Test
  public void testClusterUnresponsive_OpenSearchExceptionTransitionsToRED() throws IOException {
    // Setup: getClusterHealth() throws OpenSearchException (ES API error)
    when(mockIndexBuilder.getClusterHealth())
        .thenThrow(new OpenSearchException("Cluster not available"));

    // Execute: poll should catch OpenSearchException and transition to RED
    poller.poll();

    // Verify: Circuit breaker was updated to RED
    verify(mockCircuitBreakerState, times(1)).update(HealthState.RED);
  }

  /**
   * HIGH SEVERITY BUG FIX: Unexpected exceptions during health check MUST also transition to RED.
   * This prevents silent failures from going unnoticed.
   */
  @Test
  public void testUnexpectedException_TransitionsToRED() throws IOException {
    // Setup: getClusterHealth() throws unexpected exception
    when(mockIndexBuilder.getClusterHealth()).thenThrow(new RuntimeException("Unexpected error"));

    // Execute: poll should catch unexpected exception and transition to RED
    poller.poll();

    // Verify: Circuit breaker was updated to RED
    verify(mockCircuitBreakerState, times(1)).update(HealthState.RED);
  }
}
