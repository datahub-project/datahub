package io.datahubproject.openapi.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationSubType;
import com.linkedin.metadata.dao.throttle.APIThrottleException;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.parser.InvalidSyntaxException;
import io.datahubproject.metadata.exception.ActorAccessException;
import io.datahubproject.openapi.exception.InvalidUrnException;
import io.datahubproject.openapi.exception.UnauthorizedException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.ConversionNotSupportedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.mvc.support.DefaultHandlerExceptionResolver;

@Slf4j
@ControllerAdvice(
    basePackages = {"io.datahubproject.openapi", "com.datahub.graphql", "com.datahub.auth"})
public class GlobalControllerExceptionHandler extends DefaultHandlerExceptionResolver {

  @Autowired(required = false)
  @Nullable
  private MetricUtils metricUtils;

  @PostConstruct
  public void init() {
    log.info("GlobalControllerExceptionHandler initialized");
  }

  public GlobalControllerExceptionHandler() {
    setOrder(Ordered.LOWEST_PRECEDENCE - 1);
    setWarnLogCategory(getClass().getName());
  }

  @ExceptionHandler({ConversionFailedException.class, ConversionNotSupportedException.class})
  public ResponseEntity<String> handleConflict(RuntimeException ex) {
    return new ResponseEntity<>(ex.getMessage(), HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler({IllegalArgumentException.class, InvalidUrnException.class})
  public static ResponseEntity<Map<String, String>> handleUrnException(Exception e) {
    return new ResponseEntity<>(Map.of("error", e.getMessage()), HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler(APIThrottleException.class)
  public static ResponseEntity<Map<String, String>> handleThrottleException(
      APIThrottleException e) {

    HttpHeaders headers = new HttpHeaders();
    if (e.getDurationMs() >= 0) {
      headers.add(HttpHeaders.RETRY_AFTER, String.valueOf(e.getDurationSeconds()));
    }

    return new ResponseEntity<>(
        Map.of("error", e.getMessage()), headers, HttpStatus.TOO_MANY_REQUESTS);
  }

  @ExceptionHandler(UnauthorizedException.class)
  public static ResponseEntity<Map<String, String>> handleUnauthorizedException(
      UnauthorizedException e) {
    return new ResponseEntity<>(Map.of("error", e.getMessage()), HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler(ActorAccessException.class)
  public static ResponseEntity<Map<String, String>> actorAccessException(ActorAccessException e) {
    return new ResponseEntity<>(Map.of("error", e.getMessage()), HttpStatus.FORBIDDEN);
  }

  private static final long ASYNC_TIMEOUT_RETRY_AFTER_SECONDS = 30;

  @ExceptionHandler(AsyncRequestTimeoutException.class)
  public ResponseEntity<Map<String, String>> handleAsyncTimeout(
      AsyncRequestTimeoutException e, HttpServletRequest request) {
    log.warn(
        "Async request timeout: path={}, method={}", request.getRequestURI(), request.getMethod());
    if (metricUtils != null) {
      String pattern =
          (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
      String requestPath = (pattern != null) ? pattern : "unknown";
      metricUtils.incrementMicrometer("datahub.http.async_timeout", 1, "request_path", requestPath);
    }
    HttpHeaders headers = new HttpHeaders();
    headers.add(HttpHeaders.RETRY_AFTER, String.valueOf(ASYNC_TIMEOUT_RETRY_AFTER_SECONDS));
    return new ResponseEntity<>(
        Map.of(
            "error", "Request timed out. The operation may still be completing in the background."),
        headers,
        HttpStatus.SERVICE_UNAVAILABLE);
  }

  @ExceptionHandler(RuntimeException.class)
  public ResponseEntity<Map<String, String>> handleRuntimeException(
      RuntimeException e, HttpServletRequest request) {

    // Check if this exception originates from UrnUtils.getUrn()
    for (StackTraceElement element : e.getStackTrace()) {
      if (element.getClassName().equals("com.linkedin.common.urn.UrnUtils")
          && element.getMethodName().equals("getUrn")) {
        log.error("Invalid URN format in request: {}", request.getRequestURI(), e);
        return new ResponseEntity<>(
            Map.of("error", "Invalid URN format: " + e.getMessage()), HttpStatus.BAD_REQUEST);
      }
    }

    // For other RuntimeExceptions, let them bubble up to the generic handler
    return handleGenericException(e, request);
  }

  @Override
  protected void logException(Exception ex, HttpServletRequest request) {
    log.error("Error while resolving request: {}", request.getRequestURI(), ex);
  }

  @Override
  protected void sendServerError(
      @Nullable Exception ex, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    log.error("Error while resolving request: {}", request.getRequestURI(), ex);
    request.setAttribute("jakarta.servlet.error.exception", ex);
    response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
  }

  @ExceptionHandler(ValidationException.class)
  public ResponseEntity<Map<String, String>> handleValidationException(
      ValidationException e, HttpServletRequest request) {

    Map<String, String> response =
        new LinkedHashMap<>(Map.of("error", "Validation Error", "message", e.getMessage()));
    HttpStatus statusCode = HttpStatus.BAD_REQUEST;

    if (e.getValidationExceptionCollection() != null
        && e.getValidationExceptionCollection()
            .getSubTypes()
            .contains(ValidationSubType.AUTHORIZATION)) {
      response.put("error", "Authorization Error");
      statusCode = HttpStatus.FORBIDDEN;
    } else if (e.getValidationExceptionCollection() != null
        && e.getValidationExceptionCollection()
            .getSubTypes()
            .contains(ValidationSubType.PRECONDITION)) {
      response.put("error", "Precondition Error");
      statusCode = HttpStatus.PRECONDITION_FAILED;
    }

    // Precondition-only failures are expected during optimistic work claim (e.g. If-Version-Match
    // mismatch) and shouldn't be logged as errors since they are normal control flow.
    if (e.getValidationExceptionCollection() != null
        && Set.of(ValidationSubType.PRECONDITION)
            .equals(e.getValidationExceptionCollection().getSubTypes())) {
      log.info(
          "Conditional write precondition failed for request:{} - {}",
          request.getRequestURI(),
          e.getMessage());
    } else {
      log.error("Validation exception occurred for request:{}", request.getRequestURI(), e);
    }

    return new ResponseEntity<>(response, statusCode);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<Map<String, String>> handleGenericException(
      Exception e, HttpServletRequest request) {

    Throwable cause = e.getCause();
    while (cause != null) {
      if (cause instanceof JsonProcessingException || cause instanceof jakarta.json.JsonException) {
        return handleJsonException((Exception) cause, request);
      }
      cause = cause.getCause();
    }

    log.error("Unhandled exception occurred for request: {}", request.getRequestURI(), e);
    return new ResponseEntity<>(
        Map.of("error", "Internal server error occurred"), HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @ExceptionHandler(NoHandlerFoundException.class)
  public static ResponseEntity<Map<String, String>> handleNoHandlerFoundException(
      NoHandlerFoundException ex, HttpServletRequest request) {
    String message = String.format("No endpoint %s %s.", ex.getHttpMethod(), ex.getRequestURL());

    log.error("No handler found for request: {}", request.getRequestURI());
    return new ResponseEntity<>(Map.of("error", message), HttpStatus.NOT_FOUND);
  }

  @ExceptionHandler({JsonProcessingException.class, jakarta.json.JsonException.class})
  public ResponseEntity<Map<String, String>> handleJsonException(
      Exception e, HttpServletRequest request) {
    log.error("Invalid JSON format: {}", request.getRequestURI(), e);
    return new ResponseEntity<>(
        Map.of("error", "Invalid JSON format", "message", e.getMessage()), HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler(InvalidSyntaxException.class)
  public ResponseEntity<Map<String, String>> handleInvalidSyntaxException(
      InvalidSyntaxException e) {
    return new ResponseEntity<>(
        Map.of("error", "Invalid GraphQL syntax", "message", e.getMessage()),
        HttpStatus.BAD_REQUEST);
  }
}
