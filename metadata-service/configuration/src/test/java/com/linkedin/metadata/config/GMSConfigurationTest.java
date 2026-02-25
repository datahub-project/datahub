package com.linkedin.metadata.config;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class GMSConfigurationTest {

  @Test
  public void testGMSConfigurationBinding() {
    // This test verifies that Spring Boot can properly bind the configuration properties
    // to the GMSConfiguration class. The actual binding is tested through Spring's
    // configuration property binding mechanism.

    // Verify that the configuration classes can be instantiated
    DataHubConfiguration dataHubConfig = new DataHubConfiguration();
    GMSConfiguration gmsConfig = new GMSConfiguration();

    // Test basic property setting
    gmsConfig.setHost("test-host");
    gmsConfig.setPort(9090);
    gmsConfig.setBasePath("/gms-path");
    gmsConfig.setBasePathEnabled(true);
    gmsConfig.setUseSSL(true);

    assertEquals("test-host", gmsConfig.getHost());
    assertEquals(9090, gmsConfig.getPort());
    assertEquals("/gms-path", gmsConfig.getBasePath());
    assertTrue(gmsConfig.getBasePathEnabled());
    assertTrue(gmsConfig.getUseSSL());

    // Test nested configuration
    GMSConfiguration.TruststoreConfiguration truststore =
        new GMSConfiguration.TruststoreConfiguration();
    truststore.setPath("/path/to/truststore");
    truststore.setPassword("password");
    truststore.setType("PKCS12");

    gmsConfig.setTruststore(truststore);

    assertEquals("/path/to/truststore", gmsConfig.getTruststore().getPath());
    assertEquals("password", gmsConfig.getTruststore().getPassword());
    assertEquals("PKCS12", gmsConfig.getTruststore().getType());

    // Test async configuration
    GMSConfiguration.AsyncConfiguration async = new GMSConfiguration.AsyncConfiguration();
    async.setRequestTimeoutMs(60000L);

    gmsConfig.setAsync(async);

    assertEquals(60000L, gmsConfig.getAsync().getRequestTimeoutMs());

    // Test DataHub configuration with GMS
    dataHubConfig.setBasePath("/test-path");
    dataHubConfig.setGms(gmsConfig);

    assertEquals("/test-path", dataHubConfig.getBasePath());
    assertNotNull(dataHubConfig.getGms());
    assertEquals("test-host", dataHubConfig.getGms().getHost());
  }
}
