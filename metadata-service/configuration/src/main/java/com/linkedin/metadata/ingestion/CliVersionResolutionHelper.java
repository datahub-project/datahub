package com.linkedin.metadata.ingestion;

import com.linkedin.execution.CliVersionProvenance;
import com.linkedin.execution.CliVersionSource;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Centralizes the CLI-version resolution logic shared by the three execution-request creation paths
 * (manual trigger, scheduled trigger, test connection).
 *
 * <p>Produces a {@link Result} carrying two distinct pieces:
 *
 * <ul>
 *   <li>{@code version}: the plain CLI version string the executor will install (written to {@code
 *       args.version} on the ExecutionRequestInput aspect).
 *   <li>{@code stamp}: a structured {@link CliVersionProvenance} record describing HOW the version
 *       was chosen — written to the {@code cliVersionProvenance} field on the same aspect.
 * </ul>
 *
 * <p>The two pieces intentionally don't duplicate each other: the version string lives only in
 * {@code args.version}, and the stamp captures only the provenance fields (source tier, GMS server
 * version). Post-hoc forensics queries both via JSON paths on {@code metadata_aspect_v2}.
 *
 * <p>Resolution priority (top wins):
 *
 * <ol>
 *   <li>Per-source explicit version on {@code DataHubIngestionSourceConfig.version}
 *   <li>Matrix cohort match — first cohort whose {@code deployments} list contains this
 *       deployment's id
 *   <li>Matrix connector default — the connector's {@code _default} entry
 *   <li>Workspace default — {@code defaultCliVersion} from application.yaml
 * </ol>
 */
public final class CliVersionResolutionHelper {

  private CliVersionResolutionHelper() {}

  /**
   * Resolve a CLI version for an ingestion or test-connection request.
   *
   * @param explicitVersion the per-source version from {@code config.version}, or {@code null} /
   *     empty if unset
   * @param connectorType the source-type string from the recipe (e.g. {@code "snowflake"}), or
   *     {@code null} if not derivable (e.g. malformed test-connection recipe)
   * @param matrixService the version-matrix service; pass {@code null} for OSS callers that do not
   *     consult a matrix (e.g. unit-test setups)
   * @param defaultCliVersion the workspace-wide fallback from {@code IngestionConfiguration}
   * @param serverVersion the GMS server version (typically {@code GitVersion.getVersion()}).
   *     Stamped on every returned record regardless of which tier hit; pass {@code null} only in
   *     tests that don't care about provenance.
   * @return a {@link Result} carrying the resolved version string + the structured stamp. Never
   *     {@code null}; the {@code version} field is guaranteed non-null except when {@code
   *     defaultCliVersion} itself is null/empty (an OSS misconfiguration).
   */
  public static Result resolve(
      @Nullable String explicitVersion,
      @Nullable String connectorType,
      @Nullable IngestionVersionMatrixService matrixService,
      @Nullable String defaultCliVersion,
      @Nullable String serverVersion) {

    // Normalize the per-source version: bootstrap YAML templating can render null, empty, or
    // whitespace-only strings, and all three should mean "unset" so we fall through to the
    // matrix / workspace default. Matches the contract of
    // IngestionUtils.resolveIngestionCliVersion(...) introduced in #17471.
    final String normalizedExplicit =
        explicitVersion != null && !explicitVersion.trim().isEmpty()
            ? explicitVersion.trim()
            : null;

    if (normalizedExplicit != null) {
      return new Result(
          normalizedExplicit, stampWithSource(CliVersionSource.PER_SOURCE, serverVersion));
    }

    if (matrixService != null && connectorType != null && !connectorType.isEmpty()) {
      Optional<IngestionVersionMatrixService.MatrixResolution> matrixResult =
          matrixService.resolveVersionWithSource(connectorType);
      if (matrixResult.isPresent()) {
        IngestionVersionMatrixService.MatrixResolution r = matrixResult.get();
        CliVersionSource pdlSource =
            r.getSource() == IngestionVersionMatrixService.MatrixSourceLevel.COHORT
                ? CliVersionSource.MATRIX_COHORT
                : CliVersionSource.MATRIX_CONNECTOR_DEFAULT;
        return new Result(r.getResolved(), stampWithSource(pdlSource, serverVersion));
      }
    }

    // Default fallback. Even if `defaultCliVersion` is itself null/empty, we still emit a
    // resolution stamp so forensic queries see a deterministic answer rather than a missing field.
    return new Result(
        defaultCliVersion == null ? "" : defaultCliVersion,
        stampWithSource(CliVersionSource.WORKSPACE_DEFAULT, serverVersion));
  }

  private static CliVersionProvenance stampWithSource(
      CliVersionSource source, @Nullable String serverVersion) {
    CliVersionProvenance out = new CliVersionProvenance().setSource(source);
    if (serverVersion != null && !serverVersion.isEmpty()) {
      out.setServerVersion(serverVersion);
    }
    return out;
  }

  /**
   * Wraps the two outputs of {@link #resolve(String, String, IngestionVersionMatrixService, String,
   * String)} — the plain CLI version string (for {@code args.version}) and the structured
   * provenance stamp (for the {@code cliVersionProvenance} aspect field).
   */
  public static final class Result {
    private final String version;
    private final CliVersionProvenance stamp;

    public Result(String version, CliVersionProvenance stamp) {
      this.version = version;
      this.stamp = stamp;
    }

    /** The plain CLI version string to put in {@code args.version}. */
    public String getVersion() {
      return version;
    }

    /** The structured stamp to put on the {@code cliVersionProvenance} aspect field. */
    public CliVersionProvenance getStamp() {
      return stamp;
    }
  }
}
