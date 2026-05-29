package com.linkedin.metadata.ingestion;

import java.util.Optional;

/**
 * Resolves a CLI version for a given connector type, walking an {@link IngestionCliVersionMatrix}
 * returned by a pluggable {@link IngestionCliVersionMatrixSource}.
 *
 * <p>This class owns the <em>resolution policy</em> only — cohort ordering, allowlist matching,
 * connector-default fallback, and forensic metadata stamping. Where the matrix data comes from
 * (HTTP, a GMS metadata aspect, a config server, an in-memory test fixture, …) is the {@link
 * IngestionCliVersionMatrixSource}'s problem.
 *
 * <p>Cohort-based rollouts are aimed at multi-tenant deployments. Single-tenant installations leave
 * the deployment identifier unset, which makes cohort matching a no-op and falls through to the
 * connector's {@code _default}. When no matrix backend is configured, the factory wires a {@link
 * NoOpIngestionCliVersionMatrixSource} so {@link #resolveVersionWithSource(String)} always returns
 * {@link Optional#empty()} and callers use {@code defaultCliVersion}.
 *
 * <p>Resolution priority when picking a CLI version for an execution:
 *
 * <ol>
 *   <li>Connector-specific version stored in DataHubIngestionSourceConfig (per-source override) —
 *       handled by callers, not this service.
 *   <li>The first cohort under {@code matrix[serverVersion][connectorType].cohorts} whose {@code
 *       deployments} list contains this deployment's id.
 *   <li>{@code matrix[serverVersion][connectorType]._default}.
 *   <li>{@link Optional#empty()} — callers fall back to {@code defaultCliVersion} from
 *       application.yaml.
 * </ol>
 *
 * <p>Cohorts are evaluated in array order; the first deployments-list hit wins. An empty or missing
 * {@code deployments} list never matches.
 */
public class IngestionCliVersionMatrixService {

  private final IngestionCliVersionMatrixSource source;
  private final String serverVersion;
  private final String deploymentId;

  public IngestionCliVersionMatrixService(
      IngestionCliVersionMatrixSource source, String serverVersion, String deploymentId) {
    this.source = source;
    this.serverVersion = serverVersion;
    this.deploymentId = deploymentId;
  }

  /** Server version key this resolver matches against. */
  public String getServerVersion() {
    return serverVersion;
  }

  /**
   * Returns the CLI version to use for the given connector type, consulting the underlying matrix
   * source. Returns {@link Optional#empty()} when:
   *
   * <ul>
   *   <li>The source returned an empty matrix (no data yet, or {@link
   *       NoOpIngestionCliVersionMatrixSource})
   *   <li>The current server version has no entry in the matrix
   *   <li>The connector has no entry under the current server version
   * </ul>
   *
   * <p>Callers should fall back to {@code defaultCliVersion} when this returns empty.
   *
   * <p>For richer forensic detail (which cohort matched, when the matrix was fetched), use {@link
   * #resolveVersionWithSource(String)} instead.
   */
  public Optional<String> resolveVersion(String connectorType) {
    return resolveVersionWithSource(connectorType).map(MatrixResolution::getResolved);
  }

  /**
   * Resolves the CLI version for the given connector type and returns structured detail about which
   * level of the matrix matched (cohort vs connector default). Returns {@link Optional#empty()}
   * under the same conditions as {@link #resolveVersion(String)}.
   *
   * <p>This is the preferred API for callers that need to stamp the resolution provenance on the
   * resulting execution request (for post-hoc forensics).
   */
  public Optional<MatrixResolution> resolveVersionWithSource(String connectorType) {
    IngestionCliVersionMatrix matrix = source.getMatrix();
    ServerEntry serverEntry = matrix.getEntriesForServer(serverVersion);
    if (serverEntry == null) {
      return Optional.empty();
    }
    ConnectorEntry connectorEntry = serverEntry.getConnectorEntry(connectorType);
    if (connectorEntry == null) {
      return Optional.empty();
    }

    // Walk cohorts in array order — first cohort whose `deployments` list contains this
    // deployment's slug wins. An unset / empty deploymentId can never match a deployment entry,
    // which is the intended "fall through to _default" behavior for deployments that haven't
    // wired the env var.
    if (deploymentId != null && !deploymentId.isEmpty()) {
      for (Cohort cohort : connectorEntry.getCohorts()) {
        if (cohort.getDeployments().contains(deploymentId)) {
          return Optional.of(new MatrixResolution(cohort.getVersion(), MatrixSourceLevel.COHORT));
        }
      }
    }

    String defaultVersion = connectorEntry.getDefaultVersion();
    if (defaultVersion == null) {
      return Optional.empty();
    }
    return Optional.of(new MatrixResolution(defaultVersion, MatrixSourceLevel.CONNECTOR_DEFAULT));
  }

  /** Which level of the matrix produced a resolution. */
  public enum MatrixSourceLevel {
    /** Matched a cohort whose deployments list contained this deployment's id. */
    COHORT,
    /** Fell through to the connector's _default. */
    CONNECTOR_DEFAULT
  }

  /**
   * Structured result of a matrix resolution — just version and tier; serverVersion is stamped at
   * the helper layer.
   */
  public static final class MatrixResolution {
    private final String resolved;
    private final MatrixSourceLevel source;

    public MatrixResolution(String resolved, MatrixSourceLevel source) {
      this.resolved = resolved;
      this.source = source;
    }

    public String getResolved() {
      return resolved;
    }

    public MatrixSourceLevel getSource() {
      return source;
    }
  }
}
