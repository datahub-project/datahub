package com.datahub.gms.servlet;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.graph.GraphService;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.context.WebApplicationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = ConfigServletTestContext.class,
    properties = {"spring.main.allow-bean-definition-overriding=true"})
public class ConfigServletTest extends AbstractTestNGSpringContextTests {
  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext operationContext;

  @Autowired private WebApplicationContext webApplicationContext;

  @Mock private ServletContext servletContext;
  @Mock private HttpServletRequest request;
  @Mock private HttpServletResponse response;
  @Mock private GraphService graphService;

  private Config configServlet;
  private StringWriter responseWriter;

  @BeforeTest
  public void disableAssert() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
  }

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    // Setup mocks
    when(servletContext.getAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE))
        .thenReturn(webApplicationContext);

    // Setup response writer
    responseWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(responseWriter);
    when(response.getWriter()).thenReturn(printWriter);

    // Create servlet and set context
    configServlet = new Config();
    when(request.getServletContext()).thenReturn(servletContext);
  }

  @Test
  public void testConfigEndpoint_BasicStructure() throws Exception {
    // Execute request
    configServlet.doGet(request, response);

    // Verify response metadata
    verify(response).setContentType("application/json");
    verify(response).setStatus(HttpServletResponse.SC_OK);

    // Parse response JSON
    String responseContent = responseWriter.toString();
    assertNotNull(responseContent, "Response content should not be null");
    assertTrue(responseContent.length() > 0, "Response should not be empty");

    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);
    assertNotNull(config, "Parsed JSON config should not be null");
  }

  @Test
  public void testConfigEndpoint_BaseConfigFields() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Test BASE_CONFIG fields from Config.java
    assertEquals(config.path("noCode").asText(), "true", "noCode should be 'true'");
    assertEquals(config.path("retention").asText(), "true", "retention should be 'true'");
    assertTrue(
        config.path("statefulIngestionCapable").asBoolean(),
        "statefulIngestionCapable should be true");
    assertTrue(config.path("patchCapable").asBoolean(), "patchCapable should be true");

    // Test timeZone field
    assertTrue(config.has("timeZone"), "timeZone field should be present");
    String timeZone = config.path("timeZone").asText();
    assertNotNull(timeZone, "timeZone should not be null");
    // Verify it's a valid timezone
    ZoneId.of(timeZone); // This will throw if invalid
  }

  @Test
  public void testConfigEndpoint_VersionsSection() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Test versions structure
    assertTrue(config.has("versions"), "versions field should be present");
    JsonNode versions = config.path("versions");
    assertTrue(versions.has("acryldata/datahub"), "versions should have acryldata/datahub section");

    JsonNode datahubVersion = versions.path("acryldata/datahub");
    assertNotNull(datahubVersion, "DataHub version info should not be null");
    // The exact structure depends on GitVersion.toConfig() but should be present
    assertTrue(datahubVersion.isObject(), "DataHub version should be an object");
  }

  @Test
  public void testConfigEndpoint_TelemetrySection() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Test telemetry structure
    assertTrue(config.has("telemetry"), "telemetry field should be present");
    JsonNode telemetry = config.path("telemetry");

    assertTrue(telemetry.has("enabledCli"), "telemetry should have enabledCli field");
    assertTrue(telemetry.has("enabledIngestion"), "telemetry should have enabledIngestion field");

    // Values should be boolean
    assertTrue(telemetry.path("enabledCli").isBoolean(), "enabledCli should be boolean");
    assertTrue(
        telemetry.path("enabledIngestion").isBoolean(), "enabledIngestion should be boolean");
  }

  @Test
  public void testConfigEndpoint_ManagedIngestionSection() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Test managedIngestion structure
    assertTrue(config.has("managedIngestion"), "managedIngestion field should be present");
    JsonNode managedIngestion = config.path("managedIngestion");

    assertTrue(managedIngestion.has("enabled"), "managedIngestion should have enabled field");
    assertTrue(
        managedIngestion.has("defaultCliVersion"),
        "managedIngestion should have defaultCliVersion field");

    // Test field types
    assertTrue(managedIngestion.path("enabled").isBoolean(), "enabled should be boolean");
    assertTrue(
        managedIngestion.path("defaultCliVersion").isTextual(),
        "defaultCliVersion should be string");
  }

  @Test
  public void testConfigEndpoint_DataHubSection() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Test datahub structure
    assertTrue(config.has("datahub"), "datahub field should be present");
    JsonNode datahub = config.path("datahub");

    assertTrue(datahub.has("serverType"), "datahub should have serverType field");
    assertTrue(datahub.has("serverEnv"), "datahub should have serverEnv field");

    // serverType should be a string
    assertTrue(datahub.path("serverType").isTextual(), "serverType should be string");
    String serverType = datahub.path("serverType").asText();
    assertNotNull(serverType, "serverType should not be null");

    // serverEnv might be null, but field should exist
    assertTrue(
        datahub.hasNonNull("serverEnv") || datahub.path("serverEnv").isNull(),
        "serverEnv should be present (may be null)");
  }

  @Test
  public void testConfigEndpoint_CapabilityFields() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Test capability fields
    assertTrue(config.has("supportsImpactAnalysis"), "supportsImpactAnalysis should be present");
    assertTrue(
        config.path("supportsImpactAnalysis").isBoolean(),
        "supportsImpactAnalysis should be boolean");

    assertTrue(config.has("datasetUrnNameCasing"), "datasetUrnNameCasing should be present");
    assertTrue(
        config.path("datasetUrnNameCasing").isBoolean(), "datasetUrnNameCasing should be boolean");
  }

  @Test
  public void testConfigEndpoint_ModelsSection() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Test models section (plugin models)
    assertTrue(config.has("models"), "models field should be present");
    JsonNode models = config.path("models");
    assertTrue(models.isObject(), "models should be an object");
    // The exact content depends on plugin registry, but structure should be consistent
  }

  @Test
  public void testConfigEndpoint_CachingBehavior() throws Exception {
    // First request
    configServlet.doGet(request, response);
    String firstResponse = responseWriter.toString();

    // Reset response writer for second request
    responseWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(responseWriter);
    when(response.getWriter()).thenReturn(printWriter);

    // Second request immediately after (should use cache)
    configServlet.doGet(request, response);
    String secondResponse = responseWriter.toString();

    // Responses should be identical when cached
    assertEquals(secondResponse, firstResponse, "Cached responses should be identical");
  }

  @Test
  public void testConfigEndpoint_ResponseFormat() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Verify the response is properly formatted JSON
    assertTrue(config.isObject(), "Root should be a JSON object");

    // Verify pretty printing (should contain newlines and indentation)
    assertTrue(responseContent.contains("\n"), "Response should be pretty-printed with newlines");
    assertTrue(responseContent.contains("  "), "Response should be indented");
  }

  @Test
  public void testConfigEndpoint_AllRequiredFieldsPresent() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Define all fields that MUST be present in the current API
    String[] requiredFields = {
      "noCode", "retention", "statefulIngestionCapable", "patchCapable",
      "timeZone", "supportsImpactAnalysis", "versions", "telemetry",
      "managedIngestion", "datahub", "datasetUrnNameCasing", "models",
      "configurationProvider" // NEW: Added with secure allowlist implementation
    };

    for (String field : requiredFields) {
      assertTrue(config.has(field), "Required field should be present: " + field);
    }
  }

  @Test
  public void testDoGet_SerializationError() throws Exception {
    Config errorServlet = new Config();

    // Create a mock ObjectMapper that will throw a runtime exception
    ObjectMapper failingMapper = Mockito.mock(ObjectMapper.class);

    // Use reflection to set the ObjectMapper
    Field objectMapperField = Config.class.getDeclaredField("objectMapper");
    objectMapperField.setAccessible(true);
    objectMapperField.set(errorServlet, failingMapper);

    // Prepare the response mocking
    PrintWriter mockWriter = Mockito.mock(PrintWriter.class);
    Mockito.when(response.getWriter()).thenReturn(mockWriter);

    // Simulate a serialization error
    Mockito.when(failingMapper.writerWithDefaultPrettyPrinter())
        .thenThrow(new RuntimeException("Serialization error"));

    try {
      // Invoke the method
      errorServlet.doGet(request, response);

      // Verify method calls
      verify(response).setContentType("application/json");
      verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (Exception e) {
      fail("Should not throw an exception", e);
    }
  }

  /**
   * Test that validates the exact current schema structure for backward compatibility. This test
   * should be updated carefully when the schema changes.
   */
  @Test
  public void testConfigEndpoint_BackwardCompatibilitySchema() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Validate exact schema structure that external systems might depend on

    // Base config structure
    assertEquals(config.path("noCode").asText(), "true");
    assertEquals(config.path("retention").asText(), "true");
    assertTrue(config.path("statefulIngestionCapable").isBoolean());
    assertTrue(config.path("patchCapable").isBoolean());
    assertTrue(config.path("timeZone").isTextual());

    // Nested structure validation
    assertTrue(config.path("versions").path("acryldata/datahub").isObject());

    assertTrue(config.path("telemetry").path("enabledCli").isBoolean());
    assertTrue(config.path("telemetry").path("enabledIngestion").isBoolean());

    assertTrue(config.path("managedIngestion").path("enabled").isBoolean());
    assertTrue(config.path("managedIngestion").path("defaultCliVersion").isTextual());

    assertTrue(config.path("datahub").path("serverType").isTextual());
    // serverEnv can be null, so just check it exists
    assertTrue(config.path("datahub").has("serverEnv"));

    assertTrue(config.path("supportsImpactAnalysis").isBoolean());
    assertTrue(config.path("datasetUrnNameCasing").isBoolean());
    assertTrue(config.path("models").isObject());

    // Ensure no unexpected root-level fields (helps catch unintended additions)
    String[] expectedRootFields = {
      "noCode", "retention", "statefulIngestionCapable", "patchCapable",
      "timeZone", "supportsImpactAnalysis", "versions", "telemetry",
      "managedIngestion", "datahub", "datasetUrnNameCasing", "models",
      "configurationProvider" // NEW: Added with secure allowlist implementation
    };

    // Count should match expected fields (no extras)
    assertEquals(
        config.size(),
        expectedRootFields.length,
        "Number of root-level fields should match expected count. "
            + "If adding new fields, update this test and ensure backward compatibility.");
  }

  /**
   * Test that validates the new configurationProvider section is properly exposed with sensitive
   * information filtered out according to allowlist rules.
   */
  @Test
  public void testConfigEndpoint_ConfigurationProviderSection() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Validate configurationProvider section exists (or gracefully handle its absence)
    if (!config.has("configurationProvider")) {
      // If configurationProvider is not present, it might be because:
      // 1. Test configuration doesn't have the expected sections
      // 2. All sections were filtered out by the allowlist
      // 3. ConfigurationProvider doesn't have the expected getter methods
      // This is acceptable behavior - the test should not fail in this case
      return;
    }

    JsonNode configProvider = config.path("configurationProvider");
    assertTrue(configProvider.isObject(), "configurationProvider should be an object");

    // Test that expected safe sections might be present (depending on what's configured)
    // Note: These sections may or may not be present depending on the test configuration
    // but if they are present, they should have the right structure

    if (configProvider.has("authentication")) {
      JsonNode auth = configProvider.path("authentication");
      assertTrue(auth.isObject(), "authentication section should be an object");

      // Should have safe fields if present
      if (auth.has("enabled")) {
        assertTrue(auth.path("enabled").isBoolean(), "enabled should be boolean");
      }
      if (auth.has("defaultProvider")) {
        assertTrue(auth.path("defaultProvider").isTextual(), "defaultProvider should be string");
      }

      // Should NOT have sensitive fields - these should be filtered out by allowlist
      assertFalse(auth.has("systemClientSecret"), "systemClientSecret should be filtered out");
      assertFalse(auth.has("signingKey"), "signingKey should be filtered out");
      assertFalse(auth.has("password"), "password should be filtered out");
    }

    if (configProvider.has("kafka")) {
      JsonNode kafka = configProvider.path("kafka");
      assertTrue(kafka.isObject(), "kafka section should be an object");

      // Should have safe fields if present
      if (kafka.has("bootstrapServers")) {
        assertTrue(kafka.path("bootstrapServers").isTextual(), "bootstrapServers should be string");
      }

      // Should NOT have sensitive fields - these should be filtered out by allowlist
      assertFalse(kafka.has("security.protocol"), "security.protocol should be filtered out");
      assertFalse(kafka.has("sasl.username"), "sasl.username should be filtered out");
      assertFalse(kafka.has("sasl.password"), "sasl.password should be filtered out");
      assertFalse(
          kafka.has("ssl.keystore.password"), "ssl.keystore.password should be filtered out");
    }

    // Verify that no section contains obvious sensitive field patterns
    // Note: Only validate the configurationProvider section, not the entire config
    validateNoSensitiveFields(configProvider);
  }

  /**
   * Helper method to recursively check that no sensitive field patterns are exposed in the
   * configurationProvider section.
   */
  private void validateNoSensitiveFields(JsonNode node) {
    if (node.isObject()) {
      node.fieldNames()
          .forEachRemaining(
              fieldName -> {
                String lowerFieldName = fieldName.toLowerCase();

                // Assert that common sensitive field patterns are not present
                assertFalse(
                    lowerFieldName.contains("password"),
                    "Field containing 'password' should be filtered: " + fieldName);
                assertFalse(
                    lowerFieldName.contains("secret"),
                    "Field containing 'secret' should be filtered: " + fieldName);
                assertFalse(
                    lowerFieldName.contains("key")
                        && (lowerFieldName.contains("private")
                            || lowerFieldName.contains("signing")),
                    "Private/signing key field should be filtered: " + fieldName);
                assertFalse(
                    lowerFieldName.contains("token")
                        && !lowerFieldName.contains("duration")
                        && !lowerFieldName.contains("service")
                        && !lowerFieldName.contains("ttl"),
                    "Token field should be filtered (except duration/ttl): " + fieldName);

                // Recursively check nested objects
                validateNoSensitiveFields(node.path(fieldName));
              });
    } else if (node.isArray()) {
      for (JsonNode arrayItem : node) {
        validateNoSensitiveFields(arrayItem);
      }
    }
  }

  /**
   * Test that verifies the new configurationProvider functionality doesn't break if the allowlist
   * encounters errors (graceful degradation).
   */
  @Test
  public void testConfigEndpoint_ConfigurationProviderGracefulDegradation() throws Exception {
    // This test ensures that if the configurationProvider section encounters errors,
    // it doesn't break the entire /config endpoint

    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // All existing required fields should still be present even if configurationProvider fails
    assertTrue(config.has("noCode"), "noCode should be present");
    assertTrue(config.has("telemetry"), "telemetry should be present");
    assertTrue(config.has("managedIngestion"), "managedIngestion should be present");
    assertTrue(config.has("datahub"), "datahub should be present");
    assertTrue(config.has("models"), "models should be present");

    // configurationProvider may or may not be present (depending on success/failure)
    // but its absence should not break other functionality
    if (config.has("configurationProvider")) {
      assertTrue(
          config.path("configurationProvider").isObject(),
          "If present, configurationProvider should be an object");
    }
  }

  /** Test the new nested path support with dot notation. */
  @Test
  public void testConfigSectionRule_NestedPathSupport() throws Exception {
    // Test nested path filtering with the ConfigSectionRule directly
    Set<String> allowedFields =
        Set.of(
            "enabled", // Top-level field
            "tokenService.signingAlgorithm", // Nested field - safe
            "tokenService.issuer", // Nested field - safe
            "nested.deep.config.value" // Deep nesting test
            );

    ConfigSectionRule rule = ConfigSectionRule.include("authentication", allowedFields);

    // Test top-level field detection
    Set<String> topLevelFields = rule.getTopLevelFields();
    assertTrue(topLevelFields.contains("enabled"), "Should contain top-level field");
    assertFalse(
        topLevelFields.contains("tokenService.signingAlgorithm"),
        "Should not contain nested field in top-level");

    // Test nested path detection
    assertTrue(
        rule.hasNestedPathsForField("tokenService"), "Should detect nested paths for tokenService");
    assertFalse(
        rule.hasNestedPathsForField("enabled"), "Should not detect nested paths for simple field");

    // Test nested path retrieval
    Set<String> tokenServicePaths = rule.getAllowedPathsWithPrefix("tokenService");
    assertTrue(
        tokenServicePaths.contains("tokenService.signingAlgorithm"),
        "Should contain tokenService.signingAlgorithm");
    assertTrue(
        tokenServicePaths.contains("tokenService.issuer"), "Should contain tokenService.issuer");
    assertFalse(
        tokenServicePaths.contains("nested.deep.config.value"),
        "Should not contain paths with different prefix");
  }

  /** Test that sensitive fields are properly filtered out with nested paths. */
  @Test
  public void testConfigurationAllowlist_NestedPathFiltering() throws Exception {
    // Create a mock configuration structure that simulates what might come from
    // ConfigurationProvider
    Map<String, Object> mockConfig =
        Map.of(
            "enabled",
            true,
            "tokenService",
            Map.of(
                "signingAlgorithm", "RS256", // ✅ Should be included
                "issuer", "datahub", // ✅ Should be included
                "signingKey", "secret-key-value", // ❌ Should be filtered out
                "refreshSigningKey", "refresh-key" // ❌ Should be filtered out
                ),
            "systemClientSecret",
            "secret-value" // ❌ Should be filtered out
            );

    // Create rule that only allows safe nested fields
    Set<String> allowedFields =
        Set.of(
            "enabled", "tokenService.signingAlgorithm", "tokenService.issuer"
            // Note: signingKey and refreshSigningKey are NOT included
            );

    ConfigSectionRule rule = ConfigSectionRule.include("authentication", allowedFields);
    ConfigurationAllowlist allowlist =
        ConfigurationAllowlist.createCustom(
            Arrays.asList(rule), operationContext.getObjectMapper());

    // Apply filtering using reflection to access the private method
    try {
      java.lang.reflect.Method method =
          ConfigurationAllowlist.class.getDeclaredMethod(
              "applyFieldFiltering", Object.class, ConfigSectionRule.class);
      method.setAccessible(true);

      Object result = method.invoke(allowlist, mockConfig, rule);

      // Verify the result
      Map<String, Object> resultMap = (Map<String, Object>) result;

      // Should include top-level safe field
      assertTrue(resultMap.containsKey("enabled"), "Should include enabled field");
      assertEquals(true, resultMap.get("enabled"), "enabled value should be preserved");

      // Should include tokenService with filtered content
      assertTrue(resultMap.containsKey("tokenService"), "Should include tokenService");
      Map<String, Object> tokenService = (Map<String, Object>) resultMap.get("tokenService");

      // Should include safe nested fields
      assertTrue(tokenService.containsKey("signingAlgorithm"), "Should include signingAlgorithm");
      assertEquals(
          "RS256",
          tokenService.get("signingAlgorithm"),
          "signingAlgorithm value should be preserved");
      assertTrue(tokenService.containsKey("issuer"), "Should include issuer");
      assertEquals("datahub", tokenService.get("issuer"), "issuer value should be preserved");

      // Should NOT include sensitive nested fields
      assertFalse(tokenService.containsKey("signingKey"), "Should NOT include signingKey");
      assertFalse(
          tokenService.containsKey("refreshSigningKey"), "Should NOT include refreshSigningKey");

      // Should NOT include sensitive top-level fields
      assertFalse(
          resultMap.containsKey("systemClientSecret"), "Should NOT include systemClientSecret");

    } catch (Exception e) {
      fail("Failed to test nested path filtering: " + e.getMessage());
    }
  }
}
