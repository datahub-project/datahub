package com.linkedin.metadata.ingestion;

/**
 * {@link MatrixSource} that always returns an empty matrix. Used when no matrix backend is
 * configured (the OSS default — {@code INGESTION_VERSION_MATRIX_URL} is unset).
 *
 * <p>Always wiring a {@code NoOpMatrixSource} instead of leaving the consumer service null means
 * {@link IngestionVersionMatrixService} can rely on a non-null source without null checks on the
 * hot path, and unit tests that don't care about matrix data don't have to construct a real source.
 */
public final class NoOpMatrixSource implements MatrixSource {

  @Override
  public Matrix getMatrix() {
    return Matrix.EMPTY;
  }

  @Override
  public long getLastFetchedAtMillis() {
    return 0L;
  }
}
