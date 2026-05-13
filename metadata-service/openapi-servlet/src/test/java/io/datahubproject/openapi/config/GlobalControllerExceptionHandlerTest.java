package io.datahubproject.openapi.config;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.aspect.plugins.validation.ValidationSubType;
import com.linkedin.metadata.dao.throttle.APIThrottleException;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.parser.InvalidSyntaxException;
import io.datahubproject.metadata.exception.ActorAccessException;
import io.datahubproject.openapi.exception.InvalidUrnException;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlobalControllerExceptionHandlerTest {

  @InjectMocks private GlobalControllerExceptionHandler exceptionHandler;

  @Mock private HttpServletRequest mockRequest;

  @Mock private HttpServletResponse mockResponse;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    exceptionHandler = new GlobalControllerExceptionHandler();
  }

  @Test
  public void testInit() {
    // Test that init method runs without exception
    exceptionHandler.init();
    // No assertions needed, just verify it doesn't throw
  }

  @Test
  public void testHandleConversionFailedException() {
    ConversionFailedException ex = mock(ConversionFailedException.class);
    when(ex.getMessage()).thenReturn("Conversion failed");

    ResponseEntity<String> response = exceptionHandler.handleConflict(ex);

    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertEquals(response.getBody(), "Conversion failed");
  }

  @Test
  public void testHandleIllegalArgumentException() {
    IllegalArgumentException ex = new IllegalArgumentException("Invalid argument");

    ResponseEntity<Map<String, String>> response =
        GlobalControllerExceptionHandler.handleUrnException(ex);

    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Invalid argument");
  }

  @Test
  public void testHandleInvalidUrnException() {
    InvalidUrnException ex =
        new InvalidUrnException(
            "urn:li:dataset:(urn:li:dataPlatform:dbt,calm-pagoda-323403.jaffle_shop.orders,PROD",
            "Invalid URN");

    ResponseEntity<Map<String, String>> response =
        GlobalControllerExceptionHandler.handleUrnException(ex);

    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertNotNull(response.getBody());
    assertEquals(
        response.getBody().get("error"),
        "Invalid URN: urn:li:dataset:(urn:li:dataPlatform:dbt,calm-pagoda-323403.jaffle_shop.orders,PROD");
  }

  @Test
  public void testHandleThrottleExceptionWithDuration() {
    APIThrottleException ex = new APIThrottleException(5000L, "Too many requests");

    ResponseEntity<Map<String, String>> response =
        GlobalControllerExceptionHandler.handleThrottleException(ex);

    assertEquals(response.getStatusCode(), HttpStatus.TOO_MANY_REQUESTS);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Too many requests");

    HttpHeaders headers = response.getHeaders();
    assertNotNull(headers);
    assertEquals(headers.getFirst(HttpHeaders.RETRY_AFTER), "5");
  }

  @Test
  public void testHandleThrottleExceptionWithoutDuration() {
    APIThrottleException ex = new APIThrottleException(-1L, "Too many requests");

    ResponseEntity<Map<String, String>> response =
        GlobalControllerExceptionHandler.handleThrottleException(ex);

    assertEquals(response.getStatusCode(), HttpStatus.TOO_MANY_REQUESTS);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Too many requests");

    HttpHeaders headers = response.getHeaders();
    assertNull(headers.getFirst(HttpHeaders.RETRY_AFTER));
  }

  @Test
  public void testHandleUnauthorizedException() {
    UnauthorizedException ex = new UnauthorizedException("Unauthorized access");

    ResponseEntity<Map<String, String>> response =
        GlobalControllerExceptionHandler.handleUnauthorizedException(ex);

    assertEquals(response.getStatusCode(), HttpStatus.FORBIDDEN);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Unauthorized access");
  }

  @Test
  public void testHandleActorAccessException() {
    ActorAccessException ex = new ActorAccessException("Actor denied");

    ResponseEntity<Map<String, String>> response =
        GlobalControllerExceptionHandler.actorAccessException(ex);

    assertEquals(response.getStatusCode(), HttpStatus.FORBIDDEN);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Actor denied");
  }

  @Test
  public void testLogException() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");
    Exception ex = new Exception("Test exception");

    // This method is protected, so we call it through the instance
    exceptionHandler.logException(ex, mockRequest);

    verify(mockRequest).getRequestURI();
  }

  @Test
  public void testSendServerError() throws IOException {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");
    Exception ex = new Exception("Server error");

    exceptionHandler.sendServerError(ex, mockRequest, mockResponse);

    verify(mockRequest).setAttribute("jakarta.servlet.error.exception", ex);
    verify(mockResponse).sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
  }

  @Test
  public void testHandleValidationExceptionBasic() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");
    ValidationException ex = new ValidationException("Validation failed");

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleValidationException(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Validation Error");
    assertEquals(response.getBody().get("message"), "Validation failed");
  }

  @Test
  public void testHandleValidationExceptionWithAuthorization() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");

    ValidationExceptionCollection collection = mock(ValidationExceptionCollection.class);
    when(collection.getSubTypes()).thenReturn(Set.of(ValidationSubType.AUTHORIZATION));

    ValidationException ex = mock(ValidationException.class);
    when(ex.getMessage()).thenReturn("Authorization validation failed");
    when(ex.getValidationExceptionCollection()).thenReturn(collection);

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleValidationException(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.FORBIDDEN);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Authorization Error");
    assertEquals(response.getBody().get("message"), "Authorization validation failed");
  }

  @Test
  public void testHandleValidationExceptionWithPrecondition() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");

    ValidationExceptionCollection collection = mock(ValidationExceptionCollection.class);
    when(collection.getSubTypes()).thenReturn(Set.of(ValidationSubType.PRECONDITION));

    ValidationException ex = mock(ValidationException.class);
    when(ex.getMessage()).thenReturn("Precondition validation failed");
    when(ex.getValidationExceptionCollection()).thenReturn(collection);

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleValidationException(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.PRECONDITION_FAILED);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Precondition Error");
    assertEquals(response.getBody().get("message"), "Precondition validation failed");
  }

  @Test
  public void testHandleGenericException() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");
    Exception ex = new Exception("Generic error");

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleGenericException(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Internal server error occurred");
  }

  @Test
  public void testHandleGenericExceptionWithJsonProcessingCause() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");
    JsonProcessingException jsonEx = new JsonProcessingException("JSON error") {};
    Exception ex = new Exception("Wrapper exception", jsonEx);

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleGenericException(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Invalid JSON format");
    assertEquals(response.getBody().get("message"), "JSON error");
  }

  @Test
  public void testHandleGenericExceptionWithJakartaJsonCause() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");
    jakarta.json.JsonException jsonEx = new jakarta.json.JsonException("Jakarta JSON error");
    Exception ex = new Exception("Wrapper exception", jsonEx);

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleGenericException(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Invalid JSON format");
    assertEquals(response.getBody().get("message"), "Jakarta JSON error");
  }

  @Test
  public void testHandleGenericExceptionWithNestedJsonCause() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");
    JsonProcessingException jsonEx = new JsonProcessingException("Nested JSON error") {};
    Exception middleEx = new Exception("Middle exception", jsonEx);
    Exception ex = new Exception("Outer exception", middleEx);

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleGenericException(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Invalid JSON format");
    assertEquals(response.getBody().get("message"), "Nested JSON error");
  }

  @Test
  public void testHandleNoHandlerFoundException() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");
    NoHandlerFoundException ex = new NoHandlerFoundException("GET", "/test/endpoint", null);

    ResponseEntity<Map<String, String>> response =
        GlobalControllerExceptionHandler.handleNoHandlerFoundException(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "No endpoint GET /test/endpoint.");
  }

  @Test
  public void testHandleJsonProcessingException() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");
    JsonProcessingException ex = new JsonProcessingException("JSON parsing failed") {};

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleJsonException(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Invalid JSON format");
    assertEquals(response.getBody().get("message"), "JSON parsing failed");
  }

  @Test
  public void testHandleJakartaJsonException() {
    when(mockRequest.getRequestURI()).thenReturn("/test/endpoint");
    jakarta.json.JsonException ex = new jakarta.json.JsonException("Jakarta JSON parsing failed");

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleJsonException(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Invalid JSON format");
    assertEquals(response.getBody().get("message"), "Jakarta JSON parsing failed");
  }

  @Test
  public void testHandleInvalidSyntaxException() {
    InvalidSyntaxException ex = mock(InvalidSyntaxException.class);
    when(ex.getMessage()).thenReturn("Invalid GraphQL query syntax");

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleInvalidSyntaxException(ex);

    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertNotNull(response.getBody());
    assertEquals(response.getBody().get("error"), "Invalid GraphQL syntax");
    assertEquals(response.getBody().get("message"), "Invalid GraphQL query syntax");
  }

  @Test
  public void testHandleAsyncTimeoutReturns503WithRetryAfter() {
    when(mockRequest.getRequestURI()).thenReturn("/openapi/v3/entity/structuredProperty");
    when(mockRequest.getMethod()).thenReturn("DELETE");
    when(mockRequest.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE))
        .thenReturn("/openapi/v3/entity/{entityName}");

    AsyncRequestTimeoutException ex = new AsyncRequestTimeoutException();

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleAsyncTimeout(ex, mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.SERVICE_UNAVAILABLE);
    assertNotNull(response.getBody());
    assertEquals(
        response.getBody().get("error"),
        "Request timed out. The operation may still be completing in the background.");
    assertEquals(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER), "30");
  }

  @Test
  public void testHandleAsyncTimeoutWithMetricsEmitsCounter() throws Exception {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricUtils metricUtils = MetricUtils.builder().registry(meterRegistry).build();
    injectMetricUtils(exceptionHandler, metricUtils);

    when(mockRequest.getRequestURI()).thenReturn("/openapi/v3/entity/structuredProperty");
    when(mockRequest.getMethod()).thenReturn("DELETE");
    when(mockRequest.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE))
        .thenReturn("/openapi/v3/entity/{entityName}");

    exceptionHandler.handleAsyncTimeout(new AsyncRequestTimeoutException(), mockRequest);

    double count =
        meterRegistry
            .counter(
                "datahub.http.async_timeout", "request_path", "/openapi/v3/entity/{entityName}")
            .count();
    assertEquals(count, 1.0);
  }

  @Test
  public void testHandleAsyncTimeoutWithNullMetricsDoesNotThrow() {
    // metricUtils is null by default (no injection) — verify no NPE
    when(mockRequest.getRequestURI()).thenReturn("/test");
    when(mockRequest.getMethod()).thenReturn("GET");
    when(mockRequest.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE)).thenReturn(null);

    ResponseEntity<Map<String, String>> response =
        exceptionHandler.handleAsyncTimeout(new AsyncRequestTimeoutException(), mockRequest);

    assertEquals(response.getStatusCode(), HttpStatus.SERVICE_UNAVAILABLE);
    assertEquals(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER), "30");
  }

  @Test
  public void testHandleAsyncTimeoutWithNullPatternFallsBackToUnknown() throws Exception {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    MetricUtils metricUtils = MetricUtils.builder().registry(meterRegistry).build();
    injectMetricUtils(exceptionHandler, metricUtils);

    when(mockRequest.getRequestURI()).thenReturn("/test");
    when(mockRequest.getMethod()).thenReturn("GET");
    when(mockRequest.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE)).thenReturn(null);

    exceptionHandler.handleAsyncTimeout(new AsyncRequestTimeoutException(), mockRequest);

    double count =
        meterRegistry.counter("datahub.http.async_timeout", "request_path", "unknown").count();
    assertEquals(count, 1.0);
  }

  private static void injectMetricUtils(
      GlobalControllerExceptionHandler handler, MetricUtils metricUtils) throws Exception {
    Field field = GlobalControllerExceptionHandler.class.getDeclaredField("metricUtils");
    field.setAccessible(true);
    field.set(handler, metricUtils);
  }
}
