package io.datahubproject.openapi.exception;

/**
 * Thrown when a request body is syntactically valid but cannot be turned into something the system
 * can store — for example an OpenLineage event that deserializes cleanly but carries no usable job
 * identity. Distinct from a malformed payload (400) and from a server fault (500).
 */
public class UnprocessableEntityException extends RuntimeException {

  public UnprocessableEntityException(String message) {
    super(message);
  }
}
