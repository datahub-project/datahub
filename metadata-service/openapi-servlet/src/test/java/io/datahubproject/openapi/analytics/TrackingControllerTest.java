package io.datahubproject.openapi.analytics;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.telemetry.TrackingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.servlet.http.HttpServletRequest;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TrackingControllerTest {

  @Mock private TrackingService trackingService;

  @Mock private OperationContext systemOperationContext;

  @Mock private HttpServletRequest request;

  @Mock private Authentication authentication;

  private TrackingController controller;
  private ObjectMapper objectMapper;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    controller = new TrackingController(trackingService, systemOperationContext);
    objectMapper = new ObjectMapper();
  }

  @Test
  public void testTrackEvent() throws Exception {
    // Create a test event
    ObjectNode event = objectMapper.createObjectNode();
    event.put("type", "TestEvent");
    event.put("entityType", "dataset");
    event.put("entityUrn", "urn:li:dataset:(urn:li:dataPlatform:bigquery,example_dataset,PROD)");
    event.put("actorUrn", "urn:li:corpuser:test_user");
    event.put("customField", "test_value");

    // Mock authentication
    AuthenticationContext.setAuthentication(authentication);

    // Call the endpoint
    ResponseEntity<Void> response = controller.trackEvent(request, event);

    // Verify response
    assertEquals(response.getStatusCode().value(), 200);

    // Verify tracking service was called with the event
    verify(trackingService)
        .track(eq("TestEvent"), eq(systemOperationContext), eq(null), eq(null), eq(event));
  }

  @Test
  public void testTrackEventWithTelemetryDisabled() throws Exception {
    // Create a test event
    ObjectNode event = objectMapper.createObjectNode();
    event.put("type", "TestEvent");
    event.put("entityType", "dataset");
    event.put("entityUrn", "urn:li:dataset:(urn:li:dataPlatform:bigquery,example_dataset,PROD)");
    event.put("actorUrn", "urn:li:corpuser:test_user");
    event.put("customField", "test_value");

    // Mock authentication
    AuthenticationContext.setAuthentication(authentication);

    // Create a controller with telemetry disabled
    TrackingService disabledTrackingService =
        new TrackingService(null, null, null, null, null, null, null);
    TrackingController disabledController =
        new TrackingController(disabledTrackingService, systemOperationContext);

    // Call the endpoint
    ResponseEntity<Void> response = disabledController.trackEvent(request, event);

    // Verify response is still successful even with telemetry disabled
    assertEquals(response.getStatusCode().value(), 200);
  }

  @Test
  public void testTrackEventWithoutAuthentication() throws Exception {
    // Create a test event
    ObjectNode event = objectMapper.createObjectNode();
    event.put("type", "TestEvent");

    // Clear authentication
    AuthenticationContext.setAuthentication(null);

    // Call the endpoint
    ResponseEntity<Void> response = controller.trackEvent(request, event);

    // Verify response is unauthorized
    assertEquals(response.getStatusCode().value(), 401);
  }

  @Test
  public void testTrackEventWithInvalidEvent() throws Exception {
    // Create an invalid event (missing type)
    ObjectNode event = objectMapper.createObjectNode();
    event.put("entityType", "dataset");

    // Mock authentication
    AuthenticationContext.setAuthentication(authentication);

    // Call the endpoint
    ResponseEntity<Void> response = controller.trackEvent(request, event);

    // Verify response is bad request
    assertEquals(response.getStatusCode().value(), 400);
  }
}
