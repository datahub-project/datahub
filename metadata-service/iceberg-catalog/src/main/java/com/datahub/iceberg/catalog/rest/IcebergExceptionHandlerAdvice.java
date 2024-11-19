package com.datahub.iceberg.catalog.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.iceberg.exceptions.*;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice(basePackageClasses = AbstractIcebergController.class)
public class IcebergExceptionHandlerAdvice {

  @ExceptionHandler(AlreadyExistsException.class)
  public ResponseEntity<?> handle(AlreadyExistsException e) throws JsonProcessingException {
    return err(e, HttpStatus.CONFLICT);
  }

  @ExceptionHandler(NoSuchNamespaceException.class)
  public ResponseEntity<?> handle(NoSuchNamespaceException e) throws JsonProcessingException {
    return err(e, HttpStatus.NOT_FOUND);
  }

  @ExceptionHandler(NoSuchTableException.class)
  public ResponseEntity<?> handle(NoSuchTableException e) throws JsonProcessingException {
    return err(e, HttpStatus.NOT_FOUND);
  }

  @ExceptionHandler(NoSuchViewException.class)
  public ResponseEntity<?> handle(NoSuchViewException e) throws JsonProcessingException {
    return err(e, HttpStatus.NOT_FOUND);
  }

  @ExceptionHandler(ForbiddenException.class)
  public ResponseEntity<?> handle(ForbiddenException e) throws JsonProcessingException {
    return err(e, HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler(BadRequestException.class)
  public ResponseEntity<?> handle(BadRequestException e) throws JsonProcessingException {
    return err(e, HttpStatus.BAD_REQUEST);
  }

  private ResponseEntity<?> err(Exception e, HttpStatus errCode) throws JsonProcessingException {
    ErrorResponse err =
        ErrorResponse.builder()
            .responseCode(errCode.value())
            .withMessage(e.getMessage())
            .withType(e.getClass().getSimpleName())
            .build();
    return new ResponseEntity<>(err, errCode);
  }
}
