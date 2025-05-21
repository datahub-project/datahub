package io.datahubproject.event.exception;

public class UnsupportedTopicException extends RuntimeException {
  public UnsupportedTopicException(String topic) {
    super("Unsupported events topic provided: " + topic);
  }
}
