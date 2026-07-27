package com.linkedin.metadata.ingestion;

/**
 * Storage abstraction for the per-connector ingestion CLI version matrix.
 *
 * <p>Decouples how the matrix is fetched/stored from how it is consumed. {@link
 * IngestionCliVersionMatrixService} (the consumer) only knows that "something" returns an {@link
 * IngestionCliVersionMatrix}; implementations of this interface decide where the data comes from
 * and how/when it refreshes.
 *
 * <p>Current implementations:
 *
 * <ul>
 *   <li>{@link HttpUrlIngestionCliVersionMatrixSource} — periodic GET of a JSON document from a
 *       remote URL (S3, CDN, GitHub raw, …).
 *   <li>{@link NoOpIngestionCliVersionMatrixSource} — always returns an empty matrix. Used as the
 *       default when no source is configured, so the resolution service never needs null checks.
 * </ul>
 *
 * <p>Future implementations could include:
 *
 * <ul>
 *   <li>{@code GmsAspectIngestionCliVersionMatrixSource} — reads the matrix from a metadata aspect
 *       on a {@code globalSettings} entity inside DataHub itself. Lets workspace admins edit the
 *       matrix through the UI/GraphQL the same way they edit any other setting.
 *   <li>{@code ConfigServerIngestionCliVersionMatrixSource} — generic config-server backend (AWS
 *       AppConfig, Consul, etcd, …).
 * </ul>
 *
 * <p>Implementations are responsible for their own caching, refresh cadence, and failure handling.
 * The consumer assumes {@link #getMatrix()} is cheap to call on the hot path.
 */
public interface IngestionCliVersionMatrixSource {

  /**
   * Returns the latest available matrix snapshot. Never {@code null}; implementations should return
   * {@link IngestionCliVersionMatrix#EMPTY} if they have no data yet (e.g. initial fetch hasn't
   * completed) or if the source is intentionally a no-op.
   */
  IngestionCliVersionMatrix getMatrix();

  /**
   * Returns the epoch-millis timestamp of when the currently-cached matrix was last successfully
   * populated, or {@code 0} if no successful fetch has happened. Used by the resolution service to
   * stamp forensic metadata on the execution request (so post-hoc triage can correlate the resolved
   * version with matrix freshness).
   */
  long getLastFetchedAtMillis();
}
