package com.linkedin.metadata.billing;

/** Exception thrown when billing operations fail */
public class BillingException extends RuntimeException {

  public BillingException(String message) {
    super(message);
  }

  public BillingException(String message, Throwable cause) {
    super(message, cause);
  }
}
