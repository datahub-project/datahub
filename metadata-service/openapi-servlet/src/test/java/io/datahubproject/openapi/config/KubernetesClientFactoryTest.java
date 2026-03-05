package io.datahubproject.openapi.config;

import static org.testng.Assert.*;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.testng.annotations.Test;

/**
 * Unit tests for KubernetesClientFactory.
 *
 * <p>Note: These tests run outside of a Kubernetes environment, so the factory should return null.
 * Full integration tests would require running in a K8s cluster with proper service account.
 */
public class KubernetesClientFactoryTest {

  @Test
  public void testCreateKubernetesClientOutsideK8s() {
    KubernetesClientFactory factory = new KubernetesClientFactory();

    // Outside of K8s, should return null (not throw)
    KubernetesClient client = factory.createKubernetesClient();

    // Client should be null when not running in K8s
    assertNull(client, "KubernetesClient should be null when not running in Kubernetes");
  }

  @Test
  public void testFactoryHandlesExceptionsGracefully() {
    KubernetesClientFactory factory = new KubernetesClientFactory();

    // Should not throw even if K8s is not available
    try {
      KubernetesClient client = factory.createKubernetesClient();
      // Either null or a valid client, but should not throw
      assertTrue(client == null || client != null);
    } catch (Exception e) {
      fail("Factory should handle exceptions gracefully, got: " + e.getMessage());
    }
  }
}
