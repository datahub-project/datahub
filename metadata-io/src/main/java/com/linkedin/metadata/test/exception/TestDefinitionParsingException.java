package com.linkedin.metadata.test.exception;

/**
 * An exception to be thrown when failing to parse test definition
 */
public class TestDefinitionParsingException extends RuntimeException {

  public TestDefinitionParsingException(String message) {
    super(message);
  }

  public TestDefinitionParsingException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
