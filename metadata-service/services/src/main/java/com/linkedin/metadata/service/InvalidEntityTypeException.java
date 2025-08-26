package com.linkedin.metadata.service;

public class InvalidEntityTypeException extends RuntimeException {
  public InvalidEntityTypeException(String message) {
    super(message);
  }
}
