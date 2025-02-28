package com.linkedin.metadata.service;

public class EntityDoesNotExistException extends RuntimeException {
  public EntityDoesNotExistException(String message) {
    super(message);
  }
}
