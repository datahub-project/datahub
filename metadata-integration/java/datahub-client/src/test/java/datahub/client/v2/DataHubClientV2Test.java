package datahub.client.v2;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import datahub.client.v2.config.DataHubClientConfigV2;
import java.io.IOException;
import org.junit.Test;

/** Tests for DataHubClientV2 configuration and initialization. */
public class DataHubClientV2Test {

  @Test
  public void testClientBuilderWithMinimalConfig() throws IOException {
    DataHubClientV2 client = DataHubClientV2.builder().server("http://localhost:8080").build();

    assertNotNull(client);
    assertNotNull(client.getConfig());
    assertEquals("http://localhost:8080", client.getConfig().getServer());
    assertNotNull(client.getEmitter());
    assertNotNull(client.entities());

    client.close();
  }

  @Test
  public void testClientBuilderWithFullConfig() throws IOException {
    DataHubClientV2 client =
        DataHubClientV2.builder()
            .server("http://localhost:8080")
            .token("test-token")
            .timeoutMs(30000)
            .maxRetries(5)
            .build();

    assertNotNull(client);
    assertEquals("http://localhost:8080", client.getConfig().getServer());
    assertEquals("test-token", client.getConfig().getToken());
    assertEquals(30000, client.getConfig().getTimeoutMs());
    assertEquals(5, client.getConfig().getMaxRetries());

    client.close();
  }

  @Test
  public void testClientBuilderWithConfigObject() throws IOException {
    DataHubClientConfigV2 config =
        DataHubClientConfigV2.builder().server("http://localhost:8080").token("test-token").build();

    DataHubClientV2 client = DataHubClientV2.builder().config(config).build();

    assertNotNull(client);
    assertEquals(config, client.getConfig());

    client.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testClientBuilderWithoutServer() throws IOException {
    DataHubClientV2.builder().build();
  }

  @Test
  public void testConfigFromEnvWithMissingVariable() {
    // fromEnv() reads the process environment; the integration-test setup passes DATAHUB_SERVER
    // through to this JVM, so the "missing" case can only be checked when neither var is set.
    assumeTrue(System.getenv("DATAHUB_SERVER") == null && System.getenv("DATAHUB_GMS_URL") == null);
    try {
      DataHubClientConfigV2.fromEnv();
      fail("Should throw IllegalArgumentException when DATAHUB_SERVER is not set");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("DATAHUB_SERVER"));
    }
  }

  @Test
  public void testConfigToRestEmitterConfig() {
    DataHubClientConfigV2 config =
        DataHubClientConfigV2.builder()
            .server("http://localhost:8080")
            .token("test-token")
            .timeoutMs(15000)
            .build();

    var restConfig = config.toRestEmitterConfig();

    assertNotNull(restConfig);
    assertEquals("http://localhost:8080", restConfig.getServer());
    assertEquals("test-token", restConfig.getToken());
    assertEquals(Integer.valueOf(15), restConfig.getTimeoutSec());
  }
}
