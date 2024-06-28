package io.datahubproject.openapi;

import io.datahubproject.openapi.exception.InvalidUrnException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.ConversionNotSupportedException;
import org.springframework.core.Ordered;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.support.DefaultHandlerExceptionResolver;

@Slf4j
@ControllerAdvice
public class GlobalControllerExceptionHandler extends DefaultHandlerExceptionResolver {

  public GlobalControllerExceptionHandler() {
    setOrder(Ordered.HIGHEST_PRECEDENCE);
    setWarnLogCategory(getClass().getName());
  }

  @ExceptionHandler({ConversionFailedException.class, ConversionNotSupportedException.class})
  public ResponseEntity<String> handleConflict(RuntimeException ex) {
    return new ResponseEntity<>(ex.getMessage(), HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler(InvalidUrnException.class)
  public static ResponseEntity<Map<String, String>> handleUrnException(InvalidUrnException e) {
    return new ResponseEntity<>(Map.of("error", e.getMessage()), HttpStatus.BAD_REQUEST);
  }
}
