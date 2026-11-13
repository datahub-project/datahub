package com.linkedin.metadata.ingestion;

/**
 * {@link IngestionCliVersionMatrixSource} that always returns an empty matrix. Used when no matrix
 * backend is configured (the OSS default — {@code INGESTION_VERSION_MATRIX_URL} is unset).
 *
 * <p>Always wiring a {@code NoOpIngestionCliVersionMatrixSource} instead of leaving the consumer
 * service null means {@link IngestionCliVersionMatrixService} can rely on a non-null source without
 * null checks on the hot path, and unit tests that don't care about matrix data don't have to
 * construct a real source.
 */
public final class NoOpIngestionCliVersionMatrixSource implements IngestionCliVersionMatrixSource {

  @Override
  public IngestionCliVersionMatrix getMatrix() {
    return IngestionCliVersionMatrix.EMPTY;
  }

  @Override
  public long getLastFetchedAtMillis() {
    return 0L;
  }
}
