package com.linkedin.metadata.ingestion;

import javax.annotation.Nullable;
import org.slf4j.Logger;

/**
 * Shared structured logging for CLI version resolution at the three call sites ({@code
 * CreateIngestionExecutionRequestResolver}, {@code CreateTestConnectionRequestResolver}, {@code
 * IngestionScheduler}). Lives separately from {@link CliVersionResolutionHelper} on purpose — the
 * helper stays logging-free per review guidance, and emitting the line at the resolver layer keeps
 * each log entry tagged with the caller's class.
 *
 * <p>Why structured logging matters here: the resolution ladder spans four tiers and three
 * different trigger paths. Without a parallel-shaped log line at each call site, "which CLI did
 * this run get and why?" requires log archaeology. With it, {@code grep "Resolved ingestion CLI
 * version"} across all three resolvers' logs shows every resolution in the same shape.
 */
public final class CliVersionResolutionLogger {

  /** Trigger label for the on-demand GraphQL execution request path. */
  public static final String TRIGGER_MANUAL = "manual trigger";

  /** Trigger label for the test-connection GraphQL path. */
  public static final String TRIGGER_TEST_CONNECTION = "test-connection";

  /** Trigger label for the cron-fired scheduled execution path. */
  public static final String TRIGGER_SCHEDULED = "scheduled trigger";

  /** Identifier key for a persistent ingestion source URN. */
  public static final String IDENTIFIER_INGESTION_SOURCE = "ingestionSource";

  /** Identifier key for an execution-request URN (used when no ingestion source exists). */
  public static final String IDENTIFIER_EXECUTION_REQUEST = "executionRequest";

  private CliVersionResolutionLogger() {}

  /**
   * Emit a DEBUG line capturing which tier of the resolution ladder produced the version, and a
   * WARN line if the resolved version is empty — empty means every tier (including {@code
   * defaultCliVersion}) fell through and the executor will silently use its bundled CLI.
   *
   * @param log SLF4J logger to use. Pass the caller's {@code @Slf4j}-generated {@code log} so log
   *     lines appear under the caller's class.
   * @param trigger short label distinguishing the call site (see {@link #TRIGGER_MANUAL}, etc.)
   * @param resolution result from {@link CliVersionResolutionHelper#resolve(String, String,
   *     IngestionCliVersionMatrixService, String, String)}
   * @param connectorType source type from the recipe, may be {@code null} when the recipe lacks a
   *     parseable {@code source.type}
   * @param identifierKey self-describing key for {@code identifierValue} — typically {@link
   *     #IDENTIFIER_INGESTION_SOURCE} or {@link #IDENTIFIER_EXECUTION_REQUEST}
   * @param identifierValue the URN of the ingestion source or execution request being logged
   */
  public static void log(
      final Logger log,
      final String trigger,
      final CliVersionResolutionHelper.Result resolution,
      @Nullable final String connectorType,
      final String identifierKey,
      final String identifierValue) {
    final String serverVersion =
        resolution.getStamp().hasServerVersion()
            ? resolution.getStamp().getServerVersion()
            : "<unset>";
    final String connector = connectorType != null ? connectorType : "<unknown>";

    log.debug(
        "Resolved ingestion CLI version ({}): tier={} version={} serverVersion={} connector={} {}={}",
        trigger,
        resolution.getStamp().getSource(),
        resolution.getVersion(),
        serverVersion,
        connector,
        identifierKey,
        identifierValue);

    if (resolution.getVersion() == null || resolution.getVersion().isEmpty()) {
      log.warn(
          "Resolved CLI version is empty for {} {}={} (connector={}); the executor will fall back "
              + "to its bundled CLI. Set ingestion.defaultCliVersion or configure the matrix.",
          trigger,
          identifierKey,
          identifierValue,
          connector);
    }
  }
}
