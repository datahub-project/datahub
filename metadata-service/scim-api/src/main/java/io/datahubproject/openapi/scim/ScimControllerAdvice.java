package io.datahubproject.openapi.scim;

import lombok.extern.slf4j.Slf4j;
import org.apache.directory.scim.protocol.data.ErrorResponse;
import org.apache.directory.scim.protocol.exception.ScimException;
import org.apache.directory.scim.spec.exception.ResourceException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@Slf4j
@ControllerAdvice
public class ScimControllerAdvice extends ResponseEntityExceptionHandler {

  @ExceptionHandler({ScimException.class})
  public ResponseEntity<ErrorResponse> handleScimException(ScimException e) {
    return e.getError().toResponseEntity();
  }

  @ExceptionHandler({ResourceException.class})
  public ResponseEntity<ErrorResponse> handleResourceException(ResourceException e) {
    return new ErrorResponse(HttpStatus.valueOf(e.getStatus()), e.getMessage()).toResponseEntity();
  }
}
