package io.datahubproject.openapi.openlineage.exception;

public class OpenLineageIngestionException extends RuntimeException {
  public OpenLineageIngestionException(String message) {
    super(message);
  }

  public OpenLineageIngestionException(String message, Throwable cause) {
    super(message, cause);
  }
}
