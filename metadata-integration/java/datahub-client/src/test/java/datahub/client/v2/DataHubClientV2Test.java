package datahub.client.v2;

import static org.testng.Assert.*;

import datahub.client.v2.config.DataHubClientConfigV2;
import java.io.IOException;
import org.testng.annotations.Test;

/** Tests for DataHubClientV2 configuration and initialization. */
public class DataHubClientV2Test {

  @Test
  public void testClientBuilderWithMinimalConfig() throws IOException {
    DataHubClientV2 client = DataHubClientV2.builder().server("http://localhost:8080").build();

    assertNotNull(client);
    assertNotNull(client.getConfig());
    assertEquals(client.getConfig().getServer(), "http://localhost:8080");
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
    assertEquals(client.getConfig().getServer(), "http://localhost:8080");
    assertEquals(client.getConfig().getToken(), "test-token");
    assertEquals(client.getConfig().getTimeoutMs(), 30000);
    assertEquals(client.getConfig().getMaxRetries(), 5);

    client.close();
  }

  @Test
  public void testClientBuilderWithConfigObject() throws IOException {
    DataHubClientConfigV2 config =
        DataHubClientConfigV2.builder().server("http://localhost:8080").token("test-token").build();

    DataHubClientV2 client = DataHubClientV2.builder().config(config).build();

    assertNotNull(client);
    assertEquals(client.getConfig(), config);

    client.close();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testClientBuilderWithoutServer() throws IOException {
    DataHubClientV2.builder().build();
  }

  @Test
  public void testConfigFromEnvWithMissingVariable() {
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
    assertEquals(restConfig.getServer(), "http://localhost:8080");
    assertEquals(restConfig.getToken(), "test-token");
    assertEquals(restConfig.getTimeoutSec(), 15);
  }
}
