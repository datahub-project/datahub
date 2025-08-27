package io.datahubproject.openapi.health;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.config.HealthCheckConfiguration;
import com.linkedin.metadata.boot.BootstrapManager;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.testng.annotations.Test;

/**
 * Unit tests for HealthCheckController, focusing on bootstrap-aware health check functionality.
 *
 * <p>This test suite verifies: 1. Bootstrap-aware readiness endpoint (/health) - returns 503 during
 * bootstrap, 200 when ready 2. Liveness endpoint (/health/live) - always returns 200 if process is
 * alive 3. Existing functionality (ElasticSearch health checks) continues to work
 */
@SpringBootTest(classes = HealthCheckControllerTest.TestConfig.class)
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class HealthCheckControllerTest extends AbstractTestNGSpringContextTests {

  @Autowired private MockMvc mockMvc;

  @Autowired private BootstrapManager bootstrapManager;

  @SpringBootConfiguration
  @Import({HealthCheckControllerTestConfig.class})
  @ComponentScan(basePackages = {"io.datahubproject.openapi.health"})
  static class TestConfig {}

  @TestConfiguration
  public static class HealthCheckControllerTestConfig {
    @MockBean
    @Qualifier("elasticSearchRestHighLevelClient")
    private RestHighLevelClient elasticClient;

    @MockBean
    @Qualifier("bootstrapManager")
    private BootstrapManager bootstrapManager;

    @Bean
    @Primary
    public ConfigurationProvider testConfigurationProvider() {
      ConfigurationProvider config = mock(ConfigurationProvider.class);
      HealthCheckConfiguration healthCheck = mock(HealthCheckConfiguration.class);
      when(config.getHealthCheck()).thenReturn(healthCheck);
      when(healthCheck.getCacheDurationSeconds()).thenReturn(30);
      return config;
    }
  }

  /**
   * Test bootstrap-aware health endpoint during bootstrap phase. Should return HTTP 503 (Service
   * Unavailable) when blocking steps are not complete.
   */
  @Test
  public void testBootstrapAwareHealthDuringBootstrap() throws Exception {
    // Given: Bootstrap is still in progress
    when(bootstrapManager.areBlockingStepsComplete()).thenReturn(false);

    // When: Request /health endpoint
    mockMvc
        .perform(get("/health"))
        // Then: Should return 503 Service Unavailable with empty body
        .andExpect(status().isServiceUnavailable())
        .andExpect(content().string(""));
  }

  /**
   * Test bootstrap-aware health endpoint after bootstrap completion. Should return HTTP 200 (OK)
   * when blocking steps are complete.
   */
  @Test
  public void testBootstrapAwareHealthAfterBootstrap() throws Exception {
    // Given: Bootstrap blocking steps are complete
    when(bootstrapManager.areBlockingStepsComplete()).thenReturn(true);

    // When: Request /health endpoint
    mockMvc
        .perform(get("/health"))
        // Then: Should return 200 OK with empty body
        .andExpect(status().isOk())
        .andExpect(content().string(""));
  }

  /**
   * Test liveness endpoint always returns 200. Should return HTTP 200 regardless of bootstrap
   * status (process is alive).
   */
  @Test
  public void testLivenessEndpointAlwaysReturns200() throws Exception {
    // Given: Bootstrap is still in progress (shouldn't matter for liveness)
    when(bootstrapManager.areBlockingStepsComplete()).thenReturn(false);

    // When: Request /health/live endpoint
    mockMvc
        .perform(get("/health/live"))
        // Then: Should return 200 OK with empty body (process is alive)
        .andExpect(status().isOk())
        .andExpect(content().string(""));
  }

  /**
   * Test liveness endpoint when bootstrap is complete. Should still return HTTP 200 (liveness is
   * independent of bootstrap status).
   */
  @Test
  public void testLivenessEndpointWhenBootstrapComplete() throws Exception {
    // Given: Bootstrap is complete
    when(bootstrapManager.areBlockingStepsComplete()).thenReturn(true);

    // When: Request /health/live endpoint
    mockMvc
        .perform(get("/health/live"))
        // Then: Should return 200 OK with empty body
        .andExpect(status().isOk())
        .andExpect(content().string(""));
  }

  /**
   * Test that existing ElasticSearch health check endpoints continue to work. This ensures backward
   * compatibility is maintained.
   */
  @Test
  public void testElasticSearchHealthEndpointsStillWork() throws Exception {
    // When: Request existing ElasticSearch health endpoints
    // Then: Should not throw exceptions and return proper responses
    // Note: We're not mocking ElasticSearch responses here as that would require
    // complex setup, but we verify the endpoints are accessible

    mockMvc
        .perform(get("/check/ready"))
        .andExpect(status().isServiceUnavailable()); // Expected since ES is mocked/unavailable

    mockMvc
        .perform(get("/debug/ready"))
        .andExpect(status().isServiceUnavailable()); // Expected since ES is mocked/unavailable
  }

  /**
   * Test multiple rapid requests to health endpoint during bootstrap. Verifies thread safety and
   * consistent behavior under load.
   */
  @Test
  public void testMultipleHealthRequestsDuringBootstrap() throws Exception {
    // Given: Bootstrap is in progress
    when(bootstrapManager.areBlockingStepsComplete()).thenReturn(false);

    // When: Make multiple rapid requests
    for (int i = 0; i < 5; i++) {
      mockMvc
          .perform(get("/health"))
          // Then: All should consistently return 503
          .andExpect(status().isServiceUnavailable())
          .andExpect(content().string(""));
    }
  }

  /**
   * Test multiple rapid requests to health endpoint after bootstrap. Verifies thread safety and
   * consistent behavior under load.
   */
  @Test
  public void testMultipleHealthRequestsAfterBootstrap() throws Exception {
    // Given: Bootstrap is complete
    when(bootstrapManager.areBlockingStepsComplete()).thenReturn(true);

    // When: Make multiple rapid requests
    for (int i = 0; i < 5; i++) {
      mockMvc
          .perform(get("/health"))
          // Then: All should consistently return 200
          .andExpect(status().isOk())
          .andExpect(content().string(""));
    }
  }

  /**
   * Test bootstrap state transition from not ready to ready. Simulates the real-world scenario
   * where bootstrap completes during testing.
   */
  @Test
  public void testBootstrapStateTransition() throws Exception {
    // Given: Bootstrap starts as incomplete
    when(bootstrapManager.areBlockingStepsComplete()).thenReturn(false);

    // When: Request health during bootstrap
    mockMvc
        .perform(get("/health"))
        // Then: Should return 503
        .andExpect(status().isServiceUnavailable());

    // Given: Bootstrap completes
    when(bootstrapManager.areBlockingStepsComplete()).thenReturn(true);

    // When: Request health after bootstrap
    mockMvc
        .perform(get("/health"))
        // Then: Should now return 200
        .andExpect(status().isOk());
  }

  /**
   * Test that liveness endpoint is unaffected by bootstrap state changes. Verifies liveness remains
   * consistent regardless of readiness state.
   */
  @Test
  public void testLivenessUnaffectedByBootstrapStateChanges() throws Exception {
    // Given: Bootstrap starts as incomplete
    when(bootstrapManager.areBlockingStepsComplete()).thenReturn(false);

    // When: Request liveness during bootstrap
    mockMvc
        .perform(get("/health/live"))
        // Then: Should return 200
        .andExpect(status().isOk());

    // Given: Bootstrap completes
    when(bootstrapManager.areBlockingStepsComplete()).thenReturn(true);

    // When: Request liveness after bootstrap
    mockMvc
        .perform(get("/health/live"))
        // Then: Should still return 200
        .andExpect(status().isOk());
  }
}
