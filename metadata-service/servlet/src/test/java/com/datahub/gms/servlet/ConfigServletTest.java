package com.datahub.gms.servlet;

import static com.linkedin.metadata.Constants.ANONYMOUS_ACTOR_ID;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
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
import java.util.Collections;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.context.WebApplicationContext;
import org.testng.annotations.AfterMethod;
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

  @AfterMethod
  public void cleanup() {
    // Always clean up authentication context after each test
    AuthenticationContext.remove();
  }

  @Test
  public void testDoGet_FirstRequest() throws Exception {
    // First request should always update the configuration
    configServlet.doGet(request, response);

    // Verify response
    verify(response).setContentType("application/json");
    verify(response).setStatus(HttpServletResponse.SC_OK);

    // Parse response JSON
    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Validate configuration contents
    assertNotNull(config);
    assertTrue(config.path("statefulIngestionCapable").asBoolean());
    assertTrue(config.path("supportsImpactAnalysis").asBoolean());
    assertEquals(config.path("datahub").path("serverType").asText(), "prod");
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

  // =================================
  // Progressive Disclosure Tests (NEW)
  // =================================

  @Test
  public void testProgressiveDisclosure_AuthenticatedUser() throws Exception {
    // Setup authenticated user context
    Actor actor = new Actor(ActorType.USER, "testuser");
    Authentication authentication =
        new Authentication(actor, "credentials", Collections.emptyMap());
    AuthenticationContext.setAuthentication(authentication);

    // Execute request
    configServlet.doGet(request, response);

    // Verify response
    verify(response).setContentType("application/json");
    verify(response).setStatus(HttpServletResponse.SC_OK);

    // Parse response and check userType
    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    assertNotNull(config);
    assertEquals(
        config.path("userType").asText(), "user", "Authenticated user should have userType 'user'");
  }

  @Test
  public void testProgressiveDisclosure_AnonymousUser() throws Exception {
    // Setup anonymous user context
    Actor anonymousActor = new Actor(ActorType.USER, ANONYMOUS_ACTOR_ID);
    Authentication anonymousAuth = new Authentication(anonymousActor, "", Collections.emptyMap());
    AuthenticationContext.setAuthentication(anonymousAuth);

    // Execute request
    configServlet.doGet(request, response);

    // Verify response
    verify(response).setContentType("application/json");
    verify(response).setStatus(HttpServletResponse.SC_OK);

    // Parse response and check userType
    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    assertNotNull(config);
    assertEquals(
        config.path("userType").asText(),
        "anonymous",
        "Anonymous user should have userType 'anonymous'");
  }

  @Test
  public void testProgressiveDisclosure_NoAuthenticationContext() throws Exception {
    // No authentication context set (null)
    // AuthenticationContext.getAuthentication() will return null

    // Execute request
    configServlet.doGet(request, response);

    // Verify response
    verify(response).setContentType("application/json");
    verify(response).setStatus(HttpServletResponse.SC_OK);

    // Parse response and check userType
    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    assertNotNull(config);
    assertEquals(
        config.path("userType").asText(),
        "anonymous",
        "No authentication context should default to userType 'anonymous'");
  }

  @Test
  public void testProgressiveDisclosure_ConfigContainsUserType() throws Exception {
    // Test that the userType property is always present in response
    configServlet.doGet(request, response);

    // Parse response
    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    assertNotNull(config);
    assertTrue(config.has("userType"), "Response should always contain 'userType' property");
    assertNotNull(config.path("userType"), "userType should not be null");
    assertTrue(config.path("userType").isTextual(), "userType should be a string");
  }

  // =================================
  // Cache Behavior Tests
  // =================================

  @Test
  public void testCacheBehavior_SecondRequestUsesCache() throws Exception {
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

    // Both responses should be identical (using cached config)
    assertEquals(
        firstResponse,
        secondResponse,
        "Second request should return identical response from cache");
  }

  // =================================
  // Configuration Content Tests
  // =================================

  @Test
  public void testDoGet_ConfigurationStructure() throws Exception {
    configServlet.doGet(request, response);

    // Parse response JSON
    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    // Validate complete configuration structure
    assertNotNull(config);

    // Base config properties
    assertTrue(config.path("noCode").asBoolean());
    assertTrue(config.path("retention").asBoolean());
    assertTrue(config.path("statefulIngestionCapable").asBoolean());
    assertTrue(config.path("patchCapable").asBoolean());
    assertNotNull(config.path("timeZone").asText());

    // Progressive disclosure
    assertTrue(config.has("userType"));

    // Datahub section
    assertTrue(config.has("datahub"));
    JsonNode datahubConfig = config.path("datahub");
    assertEquals(datahubConfig.path("serverType").asText(), "prod");
    assertNotNull(datahubConfig.path("serverEnv"));

    // Telemetry section
    assertTrue(config.has("telemetry"));
    JsonNode telemetryConfig = config.path("telemetry");
    assertTrue(telemetryConfig.has("enabledCli"));
    assertTrue(telemetryConfig.has("enabledIngestion"));

    // Managed ingestion section
    assertTrue(config.has("managedIngestion"));
    JsonNode ingestionConfig = config.path("managedIngestion");
    assertTrue(ingestionConfig.has("enabled"));
    assertTrue(ingestionConfig.has("defaultCliVersion"));

    // Versions section
    assertTrue(config.has("versions"));
    JsonNode versionsConfig = config.path("versions");
    assertTrue(versionsConfig.has("acryldata/datahub"));

    // Models section (plugin models)
    assertTrue(config.has("models"));

    // Impact analysis support
    assertTrue(config.has("supportsImpactAnalysis"));

    // Dataset URN name casing
    assertTrue(config.has("datasetUrnNameCasing"));
  }
}
