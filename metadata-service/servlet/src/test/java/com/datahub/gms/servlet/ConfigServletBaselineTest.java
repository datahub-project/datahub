package com.datahub.gms.servlet;

import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.context.WebApplicationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Baseline tests for the Config servlet that capture the current output for comparison before and
 * after enhancements.
 *
 * <p>These tests serve as a regression safety net to ensure our enhancements are purely additive
 * and don't break existing functionality.
 */
@SpringBootTest(
    classes = ConfigServletTestContext.class,
    properties = {"spring.main.allow-bean-definition-overriding=true"})
public class ConfigServletBaselineTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext operationContext;

  @Autowired private WebApplicationContext webApplicationContext;

  @Mock private ServletContext servletContext;
  @Mock private HttpServletRequest request;
  @Mock private HttpServletResponse response;

  private Config configServlet;
  private StringWriter responseWriter;

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

  /**
   * Captures the current configuration output to a file for manual inspection. This helps us
   * understand exactly what the current API returns.
   */
  @Test
  public void captureCurrentConfigOutput() throws Exception {
    // Execute the config endpoint
    configServlet.doGet(request, response);
    String responseContent = responseWriter.toString();

    // Parse and validate JSON structure
    ObjectMapper mapper = operationContext.getObjectMapper();
    JsonNode config = mapper.readValue(responseContent, JsonNode.class);
    assertNotNull(config, "Config response should be valid JSON");

    // Write formatted output to file for inspection
    String prettyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);

    // Save to test resources for inspection
    Path outputPath = Paths.get("src/test/resources/baseline-config-output.json");
    try {
      Files.createDirectories(outputPath.getParent());
      Files.write(outputPath, prettyJson.getBytes());
      System.out.println("Current config output saved to: " + outputPath.toAbsolutePath());
    } catch (IOException e) {
      // If we can't write to src/test/resources, write to temp file
      Path tempFile = Files.createTempFile("baseline-config-", ".json");
      Files.write(tempFile, prettyJson.getBytes());
      System.out.println("Current config output saved to temp file: " + tempFile.toAbsolutePath());
    }

    // Basic validation that this is the expected structure
    assertTrue(config.has("noCode"), "Should have noCode field");
    assertTrue(config.has("telemetry"), "Should have telemetry section");
    assertTrue(config.has("managedIngestion"), "Should have managedIngestion section");
    assertTrue(config.has("datahub"), "Should have datahub section");
    assertTrue(config.has("versions"), "Should have versions section");
  }

  /**
   * Test that validates the JSON structure is stable and parseable. This test should continue to
   * pass even after we add new fields.
   */
  @Test
  public void testCurrentConfigStructureStability() throws Exception {
    configServlet.doGet(request, response);
    String responseContent = responseWriter.toString();

    // Parse JSON
    ObjectMapper mapper = operationContext.getObjectMapper();
    JsonNode config = mapper.readValue(responseContent, JsonNode.class);

    // Validate basic structure
    assertTrue(config.isObject(), "Root should be an object");
    assertTrue(config.size() > 0, "Should have at least one field");

    // Validate all current required top-level fields exist
    String[] requiredTopLevelFields = {
      "noCode", "retention", "statefulIngestionCapable", "patchCapable",
      "timeZone", "supportsImpactAnalysis", "versions", "telemetry",
      "managedIngestion", "datahub", "datasetUrnNameCasing", "models"
    };

    for (String field : requiredTopLevelFields) {
      assertTrue(config.has(field), "Required field missing: " + field);
    }

    // Validate telemetry structure
    JsonNode telemetry = config.path("telemetry");
    assertTrue(telemetry.isObject(), "telemetry should be an object");
    assertTrue(telemetry.has("enabledCli"), "telemetry should have enabledCli");
    assertTrue(telemetry.has("enabledIngestion"), "telemetry should have enabledIngestion");

    // Validate managedIngestion structure
    JsonNode managedIngestion = config.path("managedIngestion");
    assertTrue(managedIngestion.isObject(), "managedIngestion should be an object");
    assertTrue(managedIngestion.has("enabled"), "managedIngestion should have enabled");
    assertTrue(
        managedIngestion.has("defaultCliVersion"),
        "managedIngestion should have defaultCliVersion");

    // Validate datahub structure
    JsonNode datahub = config.path("datahub");
    assertTrue(datahub.isObject(), "datahub should be an object");
    assertTrue(datahub.has("serverType"), "datahub should have serverType");
    assertTrue(datahub.has("serverEnv"), "datahub should have serverEnv");

    // Validate versions structure
    JsonNode versions = config.path("versions");
    assertTrue(versions.isObject(), "versions should be an object");
    assertTrue(versions.has("acryldata/datahub"), "versions should have acryldata/datahub");
  }

  /**
   * Test that measures the response size and field count as a baseline. This helps us track if our
   * additions are reasonable in size.
   */
  @Test
  public void measureBaselineMetrics() throws Exception {
    configServlet.doGet(request, response);
    String responseContent = responseWriter.toString();

    ObjectMapper mapper = operationContext.getObjectMapper();
    JsonNode config = mapper.readValue(responseContent, JsonNode.class);

    // Measure current metrics
    int responseSize = responseContent.length();
    int topLevelFieldCount = config.size();
    int totalNodeCount = countAllNodes(config);

    System.out.println("=== Current Config Endpoint Baseline Metrics ===");
    System.out.println("Response size: " + responseSize + " characters");
    System.out.println("Top-level fields: " + topLevelFieldCount);
    System.out.println("Total JSON nodes: " + totalNodeCount);
    System.out.println("===============================================");

    // Set reasonable bounds - our enhancements shouldn't dramatically increase size
    assertTrue(responseSize > 100, "Response should have substantial content");
    assertTrue(responseSize < 50000, "Response shouldn't be excessively large");
    assertTrue(topLevelFieldCount >= 12, "Should have at least the known fields");
    assertTrue(topLevelFieldCount < 50, "Shouldn't have excessive top-level fields");

    // Store metrics for later comparison
    // In a real scenario, you might want to persist these for CI comparison
  }

  /** Helper method to count all nodes in a JSON tree. */
  private int countAllNodes(JsonNode node) {
    if (node == null) {
      return 0;
    }

    int count = 1; // Count this node

    if (node.isObject()) {
      for (JsonNode child : node) {
        count += countAllNodes(child);
      }
    } else if (node.isArray()) {
      for (JsonNode arrayItem : node) {
        count += countAllNodes(arrayItem);
      }
    }

    return count;
  }

  /**
   * Test that ensures the response can be round-tripped through JSON serialization. This validates
   * that our current output is properly structured.
   */
  @Test
  public void testJsonRoundTrip() throws Exception {
    configServlet.doGet(request, response);
    String originalResponse = responseWriter.toString();

    ObjectMapper mapper = operationContext.getObjectMapper();

    // Parse the original response
    JsonNode originalConfig = mapper.readValue(originalResponse, JsonNode.class);

    // Serialize it back to JSON
    String reserializedResponse =
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(originalConfig);

    // Parse the reserialized version
    JsonNode reserializedConfig = mapper.readValue(reserializedResponse, JsonNode.class);

    // They should be equivalent
    assertEquals(
        originalConfig, reserializedConfig, "Config should round-trip through JSON serialization");
  }
}
