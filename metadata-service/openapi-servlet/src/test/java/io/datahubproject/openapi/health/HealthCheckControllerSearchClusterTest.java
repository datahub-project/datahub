package io.datahubproject.openapi.health;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.config.HealthCheckConfiguration;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.gms.factory.search.SearchClusterRegistry.ClusterConnection;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.List;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

public class HealthCheckControllerSearchClusterTest {

  @Test
  public void uniqueConnectionsAreProbedAndUnhealthyClusterFailsTheCheck() throws Exception {
    SearchClientShim<?> primary = mock(SearchClientShim.class);
    SearchClientShim<?> secondary = mock(SearchClientShim.class);
    ClusterHealthResponse green = healthy();
    ClusterHealthResponse redStatus = red();
    when(primary.clusterHealth(any(), any())).thenReturn(green);
    when(secondary.clusterHealth(any(), any())).thenReturn(redStatus);

    SearchClusterRegistry registry = mock(SearchClusterRegistry.class);
    when(registry.uniqueConnections())
        .thenReturn(List.of(connection("primary", primary), connection("secondary", secondary)));

    HealthCheckController controller = controller();
    ReflectionTestUtils.setField(controller, "searchClusterRegistry", registry);
    ReflectionTestUtils.setField(controller, "elasticClient", mock(SearchClientShim.class));

    ResponseEntity<String> response = controller.getElasticDebugWithCache();
    assertEquals(response.getStatusCode(), HttpStatus.SERVICE_UNAVAILABLE);
    assertTrue(response.getBody().contains("primary="));
    assertTrue(response.getBody().contains("secondary="));
    verify(primary, times(1)).clusterHealth(any(), any());
    verify(secondary, times(1)).clusterHealth(any(), any());
  }

  @Test
  public void sharedClientIsProbedOnce() throws Exception {
    SearchClientShim<?> shared = mock(SearchClientShim.class);
    ClusterHealthResponse green = healthy();
    when(shared.clusterHealth(any(), any())).thenReturn(green);

    SearchClusterRegistry registry = mock(SearchClusterRegistry.class);
    when(registry.uniqueConnections()).thenReturn(List.of(connection("primary", shared)));

    SearchClientShim<?> unusedPrimaryBean = mock(SearchClientShim.class);
    HealthCheckController controller = controller();
    ReflectionTestUtils.setField(controller, "searchClusterRegistry", registry);
    ReflectionTestUtils.setField(controller, "elasticClient", unusedPrimaryBean);

    ResponseEntity<String> response = controller.getElasticDebugWithCache();
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    verify(shared, times(1)).clusterHealth(any(), any());
    verify(unusedPrimaryBean, never()).clusterHealth(any(), any());
  }

  private static HealthCheckController controller() {
    ConfigurationProvider config = mock(ConfigurationProvider.class);
    HealthCheckConfiguration healthCheck = mock(HealthCheckConfiguration.class);
    when(config.getHealthCheck()).thenReturn(healthCheck);
    when(healthCheck.getCacheDurationSeconds()).thenReturn(30);
    return new HealthCheckController(config);
  }

  private static ClusterConnection connection(String name, SearchClientShim<?> client) {
    return new ClusterConnection(
        name,
        mock(ElasticSearchConfiguration.class),
        client,
        mock(ESBulkProcessor.class),
        mock(ESIndexBuilder.class));
  }

  private static ClusterHealthResponse healthy() {
    ClusterHealthResponse response = mock(ClusterHealthResponse.class);
    when(response.isTimedOut()).thenReturn(false);
    when(response.getStatus()).thenReturn(ClusterHealthStatus.GREEN);
    when(response.toString()).thenReturn("green");
    return response;
  }

  private static ClusterHealthResponse red() {
    ClusterHealthResponse response = mock(ClusterHealthResponse.class);
    when(response.isTimedOut()).thenReturn(false);
    when(response.getStatus()).thenReturn(ClusterHealthStatus.RED);
    when(response.toString()).thenReturn("red");
    return response;
  }
}
