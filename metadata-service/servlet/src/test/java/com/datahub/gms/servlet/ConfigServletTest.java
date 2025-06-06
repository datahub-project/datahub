package com.datahub.gms.servlet;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
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
}
