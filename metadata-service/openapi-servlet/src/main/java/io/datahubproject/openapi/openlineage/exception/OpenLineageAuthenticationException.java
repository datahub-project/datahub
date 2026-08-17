package io.datahubproject.openapi.openlineage.exception;

public class OpenLineageAuthenticationException extends RuntimeException {
  public OpenLineageAuthenticationException() {
    super("Authentication is required for OpenLineage ingestion");
  }
}
