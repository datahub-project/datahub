package com.linkedin.metadata.system_info.collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.system_info.PropertyInfo;
import com.linkedin.metadata.system_info.PropertySourceInfo;
import com.linkedin.metadata.system_info.SystemPropertiesInfo;
import java.util.Map;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PropertiesCollectorTest {

  private PropertiesCollector propertiesCollector;
  private ConfigurableEnvironment mockEnvironment;
  private MutablePropertySources mockPropertySources;

  @BeforeMethod
  public void setUp() {
    mockEnvironment = mock(ConfigurableEnvironment.class);
    mockPropertySources = new MutablePropertySources();
    propertiesCollector = new PropertiesCollector(mockEnvironment);

    when(mockEnvironment.getPropertySources()).thenReturn(mockPropertySources);
  }

  @Test
  public void testCollectPropertiesWithFiltering() {
    // Create a test property source with both regular and sensitive properties
    TestPropertySource testSource = new TestPropertySource("test-source");
    testSource.addProperty("app.name", "datahub");
    testSource.addProperty("server.port", "8080");
    testSource.addProperty("database.password", "secret123"); // Should be filtered
    testSource.addProperty("api.secret", "api-secret-key"); // Should be filtered
    testSource.addProperty(
        "cache.client.enabled", "true"); // Should NOT be filtered (allowed prefix)

    mockPropertySources.addFirst(testSource);

    // Mock resolved values
    when(mockEnvironment.getProperty("app.name")).thenReturn("datahub");
    when(mockEnvironment.getProperty("server.port")).thenReturn("8080");
    when(mockEnvironment.getProperty("database.password")).thenReturn("secret123");
    when(mockEnvironment.getProperty("api.secret")).thenReturn("api-secret-key");
    when(mockEnvironment.getProperty("cache.client.enabled")).thenReturn("true");

    // Execute
    SystemPropertiesInfo result = propertiesCollector.collect();

    // Verify structure
    assertNotNull(result);
    assertNotNull(result.getProperties());
    assertNotNull(result.getPropertySources());
    assertTrue(result.getTotalProperties() > 0);
    assertTrue(result.getRedactedProperties() > 0);

    // Verify regular properties are not filtered
    PropertyInfo appNameProp = result.getProperties().get("app.name");
    assertNotNull(appNameProp);
    assertEquals(appNameProp.getValue(), "datahub");
    assertEquals(appNameProp.getResolvedValue(), "datahub");
    assertEquals(appNameProp.getSource(), "test-source");

    // Verify sensitive properties are filtered
    PropertyInfo passwordProp = result.getProperties().get("database.password");
    assertNotNull(passwordProp);
    assertEquals(passwordProp.getValue(), "***REDACTED***");
    assertEquals(passwordProp.getResolvedValue(), "***REDACTED***");

    PropertyInfo secretProp = result.getProperties().get("api.secret");
    assertNotNull(secretProp);
    assertEquals(secretProp.getValue(), "***REDACTED***");
    assertEquals(secretProp.getResolvedValue(), "***REDACTED***");

    // Verify allowed prefix properties are not filtered
    PropertyInfo cacheProp = result.getProperties().get("cache.client.enabled");
    assertNotNull(cacheProp);
    assertEquals(cacheProp.getValue(), "true");
    assertEquals(cacheProp.getResolvedValue(), "true");

    // Verify property sources
    assertEquals(result.getPropertySources().size(), 1);
    PropertySourceInfo sourceInfo = result.getPropertySources().get(0);
    assertEquals(sourceInfo.getName(), "test-source");
    assertEquals(sourceInfo.getType(), "TestPropertySource");
    assertEquals(sourceInfo.getPropertyCount(), 5);

    // Verify redaction count
    assertEquals(result.getRedactedProperties(), 2); // password and secret
  }

  @Test
  public void testGetPropertiesAsMap() {
    // Create a simple test property source
    TestPropertySource testSource = new TestPropertySource("simple-source");
    testSource.addProperty("app.version", "1.0.0");
    testSource.addProperty("debug.enabled", "false");

    mockPropertySources.addFirst(testSource);

    when(mockEnvironment.getProperty("app.version")).thenReturn("1.0.0");
    when(mockEnvironment.getProperty("debug.enabled")).thenReturn("false");

    // Execute
    Map<String, Object> result = propertiesCollector.getPropertiesAsMap();

    // Verify
    assertNotNull(result);
    assertEquals(result.get("app.version"), "1.0.0");
    assertEquals(result.get("debug.enabled"), "false");
  }

  // Test property source for testing
  private static class TestPropertySource extends EnumerablePropertySource<Object> {
    private final Map<String, Object> properties = new java.util.HashMap<>();

    public TestPropertySource(String name) {
      super(name);
    }

    public void addProperty(String key, Object value) {
      properties.put(key, value);
    }

    @Override
    public String[] getPropertyNames() {
      return properties.keySet().toArray(new String[0]);
    }

    @Override
    public Object getProperty(String name) {
      return properties.get(name);
    }
  }
}
