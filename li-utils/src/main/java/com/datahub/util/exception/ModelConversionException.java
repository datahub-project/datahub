package com.datahub.util.exception;

/** An exception to be thrown when Model Conversion fails. */
public class ModelConversionException extends RuntimeException {

  public ModelConversionException(String message) {
    super(message);
  }

  public ModelConversionException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
