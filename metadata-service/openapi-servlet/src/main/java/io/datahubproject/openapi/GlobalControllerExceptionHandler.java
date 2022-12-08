package io.datahubproject.openapi;

import com.linkedin.metadata.entity.exception.PreconditionFailedException;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;


@ControllerAdvice
public class GlobalControllerExceptionHandler {
  @ExceptionHandler(ConversionFailedException.class)
  public ResponseEntity<String> handleConflict(RuntimeException ex) {
    return new ResponseEntity<>(ex.getMessage(), HttpStatus.BAD_REQUEST);
  }
  @ExceptionHandler(PreconditionFailedException.class)
  public ResponseEntity<String> handlePreconditionFailed(RuntimeException ex) {
    return new ResponseEntity<>(ex.getMessage(), HttpStatus.PRECONDITION_FAILED);
  }
}
