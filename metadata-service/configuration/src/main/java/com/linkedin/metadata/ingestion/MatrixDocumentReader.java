package com.linkedin.metadata.ingestion;

/**
 * Reads the CLI version matrix document from one backend.
 *
 * <p>Everything else a polling matrix source does — caching, refresh cadence, JSON parsing,
 * last-known-good retention, failure logging — is backend-independent and lives in {@link
 * PollingIngestionCliVersionMatrixSource}. Adding a backend means implementing this and nothing
 * more, so a new store cannot accidentally acquire its own refresh semantics.
 */
public interface MatrixDocumentReader {

  /**
   * Reads the document body as a string. Throw {@link MatrixReadException} when the failure is
   * already classifiable (an HTTP status, a provider error); anything else thrown is treated as
   * transport and retried on the next refresh tick.
   */
  String read() throws Exception;

  /** The configured location, so log lines name what an operator has to go fix. */
  String displayUri();
}
