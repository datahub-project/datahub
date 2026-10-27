package com.linkedin.metadata.trace;

/** Thrown when no Kafka consumer is available within the configured borrow timeout. */
public class TraceConsumerPoolExhaustedException extends RuntimeException {

  public TraceConsumerPoolExhaustedException(String message) {
    super(message);
  }
}
