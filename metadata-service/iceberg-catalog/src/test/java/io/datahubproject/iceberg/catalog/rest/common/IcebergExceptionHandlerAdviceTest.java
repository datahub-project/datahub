package io.datahubproject.iceberg.catalog.rest.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.iceberg.exceptions.*;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IcebergExceptionHandlerAdviceTest {

  private IcebergExceptionHandlerAdvice exceptionHandler;
  private static final String TEST_ERROR_MESSAGE = "Test error message";

  @BeforeMethod
  public void setUp() {
    exceptionHandler = new IcebergExceptionHandlerAdvice();
  }

  @Test
  public void testHandleAlreadyExistsException() {
    // Arrange
    AlreadyExistsException exception = new AlreadyExistsException(TEST_ERROR_MESSAGE);

    // Act
    ResponseEntity<?> response = exceptionHandler.handle(exception);
    ErrorResponse errorResponse = (ErrorResponse) response.getBody();

    // Assert
    assertEquals(response.getStatusCode(), HttpStatus.CONFLICT);
    assertErrorResponse(
        errorResponse, HttpStatus.CONFLICT.value(), TEST_ERROR_MESSAGE, "AlreadyExistsException");
  }

  @Test
  public void testHandleNoSuchNamespaceException() {
    // Arrange
    NoSuchNamespaceException exception = new NoSuchNamespaceException(TEST_ERROR_MESSAGE);

    // Act
    ResponseEntity<?> response = exceptionHandler.handle(exception);
    ErrorResponse errorResponse = (ErrorResponse) response.getBody();

    // Assert
    assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
    assertErrorResponse(
        errorResponse,
        HttpStatus.NOT_FOUND.value(),
        TEST_ERROR_MESSAGE,
        "NoSuchNamespaceException");
  }

  @Test
  public void testHandleNoSuchTableException() {
    // Arrange
    NoSuchTableException exception = new NoSuchTableException(TEST_ERROR_MESSAGE);

    // Act
    ResponseEntity<?> response = exceptionHandler.handle(exception);
    ErrorResponse errorResponse = (ErrorResponse) response.getBody();

    // Assert
    assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
    assertErrorResponse(
        errorResponse, HttpStatus.NOT_FOUND.value(), TEST_ERROR_MESSAGE, "NoSuchTableException");
  }

  @Test
  public void testHandleNoSuchViewException() {
    // Arrange
    NoSuchViewException exception = new NoSuchViewException(TEST_ERROR_MESSAGE);

    // Act
    ResponseEntity<?> response = exceptionHandler.handle(exception);
    ErrorResponse errorResponse = (ErrorResponse) response.getBody();

    // Assert
    assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
    assertErrorResponse(
        errorResponse, HttpStatus.NOT_FOUND.value(), TEST_ERROR_MESSAGE, "NoSuchViewException");
  }

  @Test
  public void testHandleNotFoundException() {
    // Arrange
    NotFoundException exception = new NotFoundException(TEST_ERROR_MESSAGE);

    // Act
    ResponseEntity<?> response = exceptionHandler.handle(exception);
    ErrorResponse errorResponse = (ErrorResponse) response.getBody();

    // Assert
    assertEquals(response.getStatusCode(), HttpStatus.NOT_FOUND);
    assertErrorResponse(
        errorResponse, HttpStatus.NOT_FOUND.value(), TEST_ERROR_MESSAGE, "NotFoundException");
  }

  @Test
  public void testHandleForbiddenException() {
    // Arrange
    ForbiddenException exception = new ForbiddenException(TEST_ERROR_MESSAGE);

    // Act
    ResponseEntity<?> response = exceptionHandler.handle(exception);
    ErrorResponse errorResponse = (ErrorResponse) response.getBody();

    // Assert
    assertEquals(response.getStatusCode(), HttpStatus.FORBIDDEN);
    assertErrorResponse(
        errorResponse, HttpStatus.FORBIDDEN.value(), TEST_ERROR_MESSAGE, "ForbiddenException");
  }

  @Test
  public void testHandleBadRequestException() {
    // Arrange
    BadRequestException exception = new BadRequestException(TEST_ERROR_MESSAGE);

    // Act
    ResponseEntity<?> response = exceptionHandler.handle(exception);
    ErrorResponse errorResponse = (ErrorResponse) response.getBody();

    // Assert
    assertEquals(response.getStatusCode(), HttpStatus.BAD_REQUEST);
    assertErrorResponse(
        errorResponse, HttpStatus.BAD_REQUEST.value(), TEST_ERROR_MESSAGE, "BadRequestException");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testHandleGenericException() throws Exception {
    // Arrange
    RuntimeException exception = new RuntimeException(TEST_ERROR_MESSAGE);

    // Act & Assert
    exceptionHandler.handle(exception);
  }

  private void assertErrorResponse(
      ErrorResponse errorResponse, int expectedCode, String expectedMessage, String expectedType) {
    assertNotNull(errorResponse, "Error response should not be null");
    assertEquals(errorResponse.code(), expectedCode, "Response code should match");
    assertEquals(errorResponse.message(), expectedMessage, "Error message should match");
    assertEquals(errorResponse.type(), expectedType, "Error type should match");
  }
}
