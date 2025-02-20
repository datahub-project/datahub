package io.datahubproject.iceberg.catalog.rest.common;

import io.datahubproject.iceberg.catalog.rest.open.PublicIcebergApiController;
import io.datahubproject.iceberg.catalog.rest.secure.AbstractIcebergController;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.exceptions.*;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@ControllerAdvice(
    basePackageClasses = {AbstractIcebergController.class, PublicIcebergApiController.class})
@Slf4j
public class IcebergExceptionHandlerAdvice extends ResponseEntityExceptionHandler {

  @ExceptionHandler(AlreadyExistsException.class)
  public ResponseEntity<?> handle(AlreadyExistsException e) {
    return err(e, HttpStatus.CONFLICT);
  }

  @ExceptionHandler(NoSuchNamespaceException.class)
  public ResponseEntity<?> handle(NoSuchNamespaceException e) {
    return err(e, HttpStatus.NOT_FOUND);
  }

  @ExceptionHandler(NamespaceNotEmptyException.class)
  public ResponseEntity<?> handle(NamespaceNotEmptyException e) {
    return err(e, HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler(NoSuchTableException.class)
  public ResponseEntity<?> handle(NoSuchTableException e) {
    return err(e, HttpStatus.NOT_FOUND);
  }

  @ExceptionHandler(NoSuchViewException.class)
  public ResponseEntity<?> handle(NoSuchViewException e) {
    return err(e, HttpStatus.NOT_FOUND);
  }

  @ExceptionHandler(NotFoundException.class)
  public ResponseEntity<?> handle(NotFoundException e) {
    return err(e, HttpStatus.NOT_FOUND);
  }

  @ExceptionHandler(ForbiddenException.class)
  public ResponseEntity<?> handle(ForbiddenException e) {
    return err(e, HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler(BadRequestException.class)
  public ResponseEntity<?> handle(BadRequestException e) {
    return err(e, HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<?> handle(Exception e) throws Exception {
    log.error("Server exception", e);
    throw e;
  }

  private ResponseEntity<?> err(Exception e, HttpStatus errCode) {
    ErrorResponse err =
        ErrorResponse.builder()
            .responseCode(errCode.value())
            .withMessage(e.getMessage())
            .withType(e.getClass().getSimpleName())
            .build();
    return new ResponseEntity<>(err, errCode);
  }
}
