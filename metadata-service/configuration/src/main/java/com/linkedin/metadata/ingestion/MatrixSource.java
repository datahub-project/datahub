package com.linkedin.metadata.ingestion;

/**
 * Storage abstraction for the per-connector CLI version matrix.
 *
 * <p>Decouples how the matrix is fetched/stored from how it is consumed. {@link
 * IngestionVersionMatrixService} (the consumer) only knows that "something" returns a {@link
 * Matrix}; implementations of this interface decide where the data comes from and how/when it
 * refreshes.
 *
 * <p>Current implementations:
 *
 * <ul>
 *   <li>{@link HttpUrlMatrixSource} — periodic GET of a JSON document from a remote URL (S3, CDN,
 *       GitHub raw, …).
 *   <li>{@link NoOpMatrixSource} — always returns an empty matrix. Used as the default when no
 *       source is configured, so the resolution service never needs null checks.
 * </ul>
 *
 * <p>Future implementations could include:
 *
 * <ul>
 *   <li>{@code GmsAspectMatrixSource} — reads the matrix from a metadata aspect on a {@code
 *       globalSettings} entity inside DataHub itself. Lets workspace admins edit the matrix through
 *       the UI/GraphQL the same way they edit any other setting.
 *   <li>{@code ConfigServerMatrixSource} — generic config-server backend (AWS AppConfig, Consul,
 *       etcd, …).
 * </ul>
 *
 * <p>Implementations are responsible for their own caching, refresh cadence, and failure handling.
 * The consumer assumes {@link #getMatrix()} is cheap to call on the hot path.
 */
public interface MatrixSource {

  /**
   * Returns the latest available matrix snapshot. Never {@code null}; implementations should return
   * {@link Matrix#EMPTY} if they have no data yet (e.g. initial fetch hasn't completed) or if the
   * source is intentionally a no-op.
   */
  Matrix getMatrix();

  /**
   * Returns the epoch-millis timestamp of when the currently-cached matrix was last successfully
   * populated, or {@code 0} if no successful fetch has happened. Used by the resolution service to
   * stamp forensic metadata on the execution request (so post-hoc triage can correlate the resolved
   * version with matrix freshness).
   */
  long getLastFetchedAtMillis();
}
