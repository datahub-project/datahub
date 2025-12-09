package com.linkedin.gms;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.core.env.Environment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for CommonApplicationConfig to verify Jetty server security configurations,
 * specifically that server identification headers are suppressed.
 */
public class CommonApplicationConfigTest {

  private CommonApplicationConfig config;
  private Environment mockEnvironment;

  @BeforeMethod
  public void setup() {
    config = new CommonApplicationConfig();
    mockEnvironment = mock(Environment.class);

    // Mock environment properties with defaults
    when(mockEnvironment.getProperty(eq("server.port"), eq(Integer.class), eq(8080)))
        .thenReturn(8080);
    when(mockEnvironment.getProperty(eq("server.ssl.port"), eq(Integer.class), eq(8443)))
        .thenReturn(8443);
    when(mockEnvironment.getProperty(eq("server.ssl.key-store"))).thenReturn(null);
    when(mockEnvironment.getProperty(eq("server.ssl.key-store-password"))).thenReturn(null);

    // Use reflection to set the mocked environment
    try {
      java.lang.reflect.Field field = CommonApplicationConfig.class.getDeclaredField("environment");
      field.setAccessible(true);
      field.set(config, mockEnvironment);
    } catch (Exception e) {
      throw new RuntimeException("Failed to inject mocked environment", e);
    }
  }

  @Test
  public void testHttpConfigurationSettings() {
    // Get the customizer bean
    WebServerFactoryCustomizer<JettyServletWebServerFactory> customizer = config.jettyCustomizer();
    assertNotNull(customizer, "Jetty customizer should not be null");

    // Create a test factory and apply customization
    JettyServletWebServerFactory factory = new JettyServletWebServerFactory();
    customizer.customize(factory);

    // Create a test server to extract the configuration
    Server server = new Server();

    // Apply the server customizers from the factory
    factory.getServerCustomizers().forEach(sc -> sc.customize(server));

    // Verify that connectors were configured
    assertNotNull(server.getConnectors(), "Server should have connectors configured");
    assertTrue(server.getConnectors().length > 0, "Server should have at least one connector");

    // Get the HTTP configuration from the connector
    ServerConnector connector = (ServerConnector) server.getConnectors()[0];
    HttpConnectionFactory connectionFactory =
        connector.getConnectionFactory(HttpConnectionFactory.class);
    assertNotNull(connectionFactory, "HttpConnectionFactory should not be null");

    HttpConfiguration httpConfig = connectionFactory.getHttpConfiguration();
    assertNotNull(httpConfig, "HttpConfiguration should not be null");

    // Verify all HttpConfiguration settings to ensure codecov coverage
    // This covers: httpConfig.setRequestHeaderSize(32768)
    int requestHeaderSize = httpConfig.getRequestHeaderSize();
    assertEquals(requestHeaderSize, 32768, "Request header size should be set to 32768 bytes");

    // This covers: httpConfig.setSendServerVersion(false)
    boolean sendServerVersion = httpConfig.getSendServerVersion();
    assertFalse(
        sendServerVersion,
        "Server version header (Server:) should be disabled to prevent information disclosure");

    // This covers: httpConfig.setSendDateHeader(false)
    boolean sendDateHeader = httpConfig.getSendDateHeader();
    assertFalse(sendDateHeader, "Date header should be disabled to prevent information disclosure");

    // This covers: httpConfig.setUriCompliance(...)
    assertNotNull(httpConfig.getUriCompliance(), "URI compliance should be configured");
  }
}
