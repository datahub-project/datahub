package io.datahubproject.openapi.openlineage.exception;

import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.openlineage.controller.LineageApiImpl;
import io.datahubproject.openapi.openlineage.validation.OpenLineageValidationError;
import io.datahubproject.openlineage.model.OpenLineageErrorResponse;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice(assignableTypes = LineageApiImpl.class)
public class OpenLineageControllerExceptionHandler {
  @ExceptionHandler(InvalidOpenLineageEventException.class)
  public ResponseEntity<OpenLineageErrorResponse> handleInvalidEvent(
      InvalidOpenLineageEventException exception) {
    List<OpenLineageValidationError> errors = exception.getValidationErrors();
    if (errors.isEmpty()) {
      String path = exception.getField() == null ? "$" : "$." + exception.getField();
      errors = List.of(new OpenLineageValidationError(path, "invalid", null, null));
    }
    return invalidEvent(errors);
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<OpenLineageErrorResponse> handleUnreadableBody(
      HttpMessageNotReadableException exception) {
    return invalidEvent(List.of(new OpenLineageValidationError("$", "malformedJson", null, null)));
  }

  @ExceptionHandler(OpenLineageUnsupportedMediaTypeException.class)
  public ResponseEntity<OpenLineageErrorResponse> handleUnsupportedMediaType() {
    return new ResponseEntity<>(
        response(
            "UNSUPPORTED_MEDIA_TYPE", "OpenLineage requests must use application/json", Map.of()),
        HttpStatus.UNSUPPORTED_MEDIA_TYPE);
  }

  @ExceptionHandler(OpenLineageAuthenticationException.class)
  public ResponseEntity<OpenLineageErrorResponse> handleAuthentication(
      OpenLineageAuthenticationException exception) {
    return new ResponseEntity<>(
        response("AUTHENTICATION_REQUIRED", exception.getMessage(), Map.of()),
        HttpStatus.UNAUTHORIZED);
  }

  @ExceptionHandler(UnauthorizedException.class)
  public ResponseEntity<OpenLineageErrorResponse> handleAuthorization(
      UnauthorizedException exception) {
    return new ResponseEntity<>(
        response("AUTHORIZATION_DENIED", exception.getMessage(), Map.of()), HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler(OpenLineageIngestionException.class)
  public ResponseEntity<OpenLineageErrorResponse> handleIngestion(
      OpenLineageIngestionException exception) {
    Map<String, Object> details = new LinkedHashMap<>();
    if (exception.getCause() != null) {
      details.put("exception", exception.getCause().getClass().getSimpleName());
    }
    return new ResponseEntity<>(
        response("INGESTION_FAILED", exception.getMessage(), details),
        HttpStatus.INTERNAL_SERVER_ERROR);
  }

  private static ResponseEntity<OpenLineageErrorResponse> invalidEvent(
      List<OpenLineageValidationError> errors) {
    return new ResponseEntity<>(
        response(
            "INVALID_EVENT", "Invalid OpenLineage event", Map.of("errors", List.copyOf(errors))),
        HttpStatus.BAD_REQUEST);
  }

  private static OpenLineageErrorResponse response(
      String code, String message, Map<String, Object> details) {
    OpenLineageErrorResponse response = new OpenLineageErrorResponse();
    response.setCode(code);
    response.setMessage(message);
    response.setDetails(details);
    return response;
  }
}
