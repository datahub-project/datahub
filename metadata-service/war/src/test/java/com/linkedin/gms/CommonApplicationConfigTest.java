package com.linkedin.gms;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.core.env.Environment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CommonApplicationConfigTest {

  private CommonApplicationConfig config;
  private Environment mockEnvironment;

  @BeforeMethod
  public void setup() {
    config = new CommonApplicationConfig();
    mockEnvironment = mock(Environment.class);

    // Mock environment properties with defaults (HTTP-only configuration)
    when(mockEnvironment.getProperty(eq("server.port"), eq(Integer.class), eq(8080)))
        .thenReturn(8080);
    when(mockEnvironment.getProperty(eq("server.ssl.port"), eq(Integer.class), eq(8443)))
        .thenReturn(8443);
    when(mockEnvironment.getProperty(eq("server.ssl.key-store"))).thenReturn(null);
    when(mockEnvironment.getProperty(eq("server.ssl.key-store-password"))).thenReturn(null);
    when(mockEnvironment.getProperty(eq("server.ssl.key-store-type"), eq("PKCS12")))
        .thenReturn("PKCS12");
    when(mockEnvironment.getProperty(eq("server.ssl.key-alias"))).thenReturn(null);

    // Use reflection to set the mocked environment
    try {
      java.lang.reflect.Field field = CommonApplicationConfig.class.getDeclaredField("environment");
      field.setAccessible(true);
      field.set(config, mockEnvironment);
    } catch (Exception e) {
      throw new RuntimeException("Failed to inject mocked environment", e);
    }
  }

  private HttpConfiguration getHttpConfiguration() {
    WebServerFactoryCustomizer<JettyServletWebServerFactory> customizer = config.jettyCustomizer();
    assertNotNull(customizer, "Jetty customizer should not be null");

    JettyServletWebServerFactory factory = new JettyServletWebServerFactory();
    customizer.customize(factory);

    Server server = new Server();
    factory.getServerCustomizers().forEach(sc -> sc.customize(server));

    assertNotNull(server.getConnectors(), "Server should have connectors configured");
    assertTrue(server.getConnectors().length > 0, "Server should have at least one connector");

    ServerConnector connector = (ServerConnector) server.getConnectors()[0];
    HttpConnectionFactory connectionFactory =
        connector.getConnectionFactory(HttpConnectionFactory.class);
    assertNotNull(connectionFactory, "HttpConnectionFactory should not be null");

    HttpConfiguration httpConfig = connectionFactory.getHttpConfiguration();
    assertNotNull(httpConfig, "HttpConfiguration should not be null");

    return httpConfig;
  }

  @Test
  public void testJettyCustomizerBeanCreation() {
    WebServerFactoryCustomizer<JettyServletWebServerFactory> customizer = config.jettyCustomizer();
    assertNotNull(customizer, "Jetty customizer bean should be created");
  }

  @Test
  public void testRequestHeaderSizeConfiguration() {
    HttpConfiguration httpConfig = getHttpConfiguration();

    int requestHeaderSize = httpConfig.getRequestHeaderSize();
    assertEquals(
        requestHeaderSize,
        32768,
        "Request header size should be set to 32768 bytes for large JWT tokens");
  }

  @Test
  public void testServerVersionDisclosureDisabled() {
    HttpConfiguration httpConfig = getHttpConfiguration();

    boolean sendServerVersion = httpConfig.getSendServerVersion();
    assertFalse(
        sendServerVersion,
        "Server version header (Server:) must be disabled to prevent information disclosure");
  }

  @Test
  public void testDateHeaderDisabled() {
    HttpConfiguration httpConfig = getHttpConfiguration();

    boolean sendDateHeader = httpConfig.getSendDateHeader();
    assertFalse(
        sendDateHeader,
        "Date header must be disabled to prevent information disclosure and timing attacks");
  }

  @Test
  public void testUriComplianceConfiguration() {
    HttpConfiguration httpConfig = getHttpConfiguration();

    assertNotNull(httpConfig.getUriCompliance(), "URI compliance should be configured");
  }

  @Test
  public void testHttpPortConfiguration() {
    WebServerFactoryCustomizer<JettyServletWebServerFactory> customizer = config.jettyCustomizer();
    JettyServletWebServerFactory factory = new JettyServletWebServerFactory();
    customizer.customize(factory);

    Server server = new Server();
    factory.getServerCustomizers().forEach(sc -> sc.customize(server));

    ServerConnector connector = (ServerConnector) server.getConnectors()[0];
    assertEquals(connector.getPort(), 8080, "HTTP port should be set to 8080");
  }

  @Test
  public void testHttpsPortPropertyRead() {
    // Trigger the customizer to ensure properties are read
    WebServerFactoryCustomizer<JettyServletWebServerFactory> customizer = config.jettyCustomizer();
    JettyServletWebServerFactory factory = new JettyServletWebServerFactory();
    customizer.customize(factory);
    Server server = new Server();
    factory.getServerCustomizers().forEach(sc -> sc.customize(server));

    // Verify that the HTTPS port property is read correctly
    verify(mockEnvironment).getProperty(eq("server.ssl.port"), eq(Integer.class), eq(8443));
  }

  @Test
  public void testSslPropertiesRead() {
    // Trigger the customizer to ensure properties are read
    WebServerFactoryCustomizer<JettyServletWebServerFactory> customizer = config.jettyCustomizer();
    JettyServletWebServerFactory factory = new JettyServletWebServerFactory();
    customizer.customize(factory);
    Server server = new Server();
    factory.getServerCustomizers().forEach(sc -> sc.customize(server));

    // Verify that SSL-related properties are read from environment
    // This covers the conditional SSL configuration branches (lines 105-106, 107-108, 124)
    verify(mockEnvironment).getProperty(eq("server.ssl.key-store"));
    verify(mockEnvironment).getProperty(eq("server.ssl.key-store-password"));
    verify(mockEnvironment).getProperty(eq("server.ssl.key-store-type"), eq("PKCS12"));
    verify(mockEnvironment).getProperty(eq("server.ssl.key-alias"));
  }

  @Test
  public void testSingleConnectorConfiguration() {
    WebServerFactoryCustomizer<JettyServletWebServerFactory> customizer = config.jettyCustomizer();
    JettyServletWebServerFactory factory = new JettyServletWebServerFactory();
    customizer.customize(factory);

    Server server = new Server();
    factory.getServerCustomizers().forEach(sc -> sc.customize(server));

    Connector[] connectors = server.getConnectors();
    assertNotNull(connectors, "Connectors should be configured");
    assertEquals(
        connectors.length, 1, "Should have exactly one connector (HTTP or HTTPS, not both)");
  }

  @Test
  public void testJmxConfigurationApplied() {
    WebServerFactoryCustomizer<JettyServletWebServerFactory> customizer = config.jettyCustomizer();
    JettyServletWebServerFactory factory = new JettyServletWebServerFactory();
    customizer.customize(factory);

    Server server = new Server();
    factory.getServerCustomizers().forEach(sc -> sc.customize(server));

    // Verify JMX bean was added to server
    assertNotNull(server.getBeans(), "Server should have beans configured");
    assertTrue(
        server.getBeans().stream()
            .anyMatch(bean -> bean.getClass().getName().contains("MBeanContainer")),
        "Server should have MBeanContainer configured for JMX monitoring");
  }
}
