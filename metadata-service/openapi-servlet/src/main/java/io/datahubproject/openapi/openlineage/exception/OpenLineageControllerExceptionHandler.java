package io.datahubproject.openapi.openlineage.exception;

import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.openlineage.controller.LineageApiImpl;
import io.datahubproject.openapi.openlineage.validation.OpenLineageValidationError;
import io.datahubproject.openlineage.model.OpenLineageErrorResponse;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice(assignableTypes = LineageApiImpl.class)
public class OpenLineageControllerExceptionHandler {
  @ExceptionHandler(InvalidOpenLineageEventException.class)
  public ResponseEntity<OpenLineageErrorResponse> handleInvalidEvent(
      InvalidOpenLineageEventException exception) {
    return invalidEvent(exception.getValidationErrors());
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  public ResponseEntity<OpenLineageErrorResponse> handleUnreadableBody() {
    return invalidEvent(List.of(new OpenLineageValidationError("$", "malformedJson", null, null)));
  }

  @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
  public ResponseEntity<OpenLineageErrorResponse> handleUnsupportedMediaType() {
    return new ResponseEntity<>(
        response(
            "UNSUPPORTED_MEDIA_TYPE", "OpenLineage requests must use application/json", Map.of()),
        HttpStatus.UNSUPPORTED_MEDIA_TYPE);
  }

  @ExceptionHandler(UnauthorizedException.class)
  public ResponseEntity<OpenLineageErrorResponse> handleAuthorization(
      UnauthorizedException exception) {
    return new ResponseEntity<>(
        response("AUTHORIZATION_DENIED", exception.getMessage(), Map.of()), HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<OpenLineageErrorResponse> handleUnexpectedFailure(Exception exception) {
    log.error("Failed to ingest OpenLineage event", exception);
    return new ResponseEntity<>(
        response("INGESTION_FAILED", "Failed to ingest OpenLineage event", Map.of()),
        HttpStatus.INTERNAL_SERVER_ERROR);
  }

  private static ResponseEntity<OpenLineageErrorResponse> invalidEvent(
      List<OpenLineageValidationError> errors) {
    List<OpenLineageValidationError> validationErrors =
        errors.isEmpty()
            ? List.of(new OpenLineageValidationError("$", "invalid", null, null))
            : List.copyOf(errors);
    return new ResponseEntity<>(
        response("INVALID_EVENT", "Invalid OpenLineage event", Map.of("errors", validationErrors)),
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
