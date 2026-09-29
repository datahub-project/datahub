package com.datahub.gms.servlet;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.micrometer.core.instrument.Clock;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.context.WebApplicationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Tests Config servlet with pgQueue enabled to verify priority bands are exposed via /config
 * endpoint.
 */
@SpringBootTest(
    classes = ConfigServletTestContext.class,
    properties = {
      "spring.main.allow-bean-definition-overriding=true",
      "postgres.pgQueue.enabled=true",
      "postgres.pgQueue.topicDefaults.priorityBands="
          + "[{\"range\":[0,3],\"weight\":70},{\"range\":[4,6],\"weight\":20},{\"range\":[7,9],\"weight\":10}]"
    })
public class ConfigServletPgQueueTest extends AbstractTestNGSpringContextTests {
  @Autowired
  @Qualifier("systemOperationContext")
  private OperationContext operationContext;

  @Autowired private WebApplicationContext webApplicationContext;

  @MockitoBean public Clock clock;
  @MockitoBean public SystemTelemetryContext systemTelemetryContext;

  @MockitoBean(name = "searchClientShim", answers = Answers.RETURNS_MOCKS)
  SearchClientShim<?> searchClientShim;

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

    when(servletContext.getAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE))
        .thenReturn(webApplicationContext);

    responseWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(responseWriter);
    when(response.getWriter()).thenReturn(printWriter);

    configServlet = new Config();
    when(request.getServletContext()).thenReturn(servletContext);
  }

  @Test
  public void testDoGet_PgQueueEnabled_ExposesPriorityBands() throws Exception {
    configServlet.doGet(request, response);

    verify(response).setContentType("application/json");
    verify(response).setStatus(HttpServletResponse.SC_OK);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);

    assertNotNull(config);
    assertTrue(config.has("pgQueue"), "Response should contain pgQueue section when enabled");

    JsonNode pgQueueNode = config.path("pgQueue");
    assertTrue(pgQueueNode.has("priorityBands"), "pgQueue section should contain priorityBands");

    String bandsJson = pgQueueNode.path("priorityBands").asText();
    assertNotNull(bandsJson);
    assertTrue(bandsJson.contains("\"range\""), "priorityBands should be a valid JSON array");
    assertTrue(bandsJson.contains("\"weight\""), "priorityBands should contain weight fields");
  }

  @Test
  public void testDoGet_PgQueuePriorityBands_ParseableFormat() throws Exception {
    configServlet.doGet(request, response);

    String responseContent = responseWriter.toString();
    JsonNode config = operationContext.getObjectMapper().readValue(responseContent, JsonNode.class);
    String bandsJson = config.path("pgQueue").path("priorityBands").asText();

    JsonNode bandsArray = operationContext.getObjectMapper().readTree(bandsJson);
    assertTrue(bandsArray.isArray(), "priorityBands should parse as a JSON array");
    assertEquals(bandsArray.size(), 3, "Default config should have 3 priority bands");

    JsonNode firstBand = bandsArray.get(0);
    assertTrue(firstBand.has("range"), "Each band should have a range");
    assertTrue(firstBand.has("weight"), "Each band should have a weight");
    assertEquals(firstBand.path("weight").asInt(), 70);
  }
}
