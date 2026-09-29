package com.linkedin.metadata.ingestion;

import javax.annotation.Nullable;
import org.slf4j.Logger;

/**
 * Shared structured logging for CLI version resolution at the three call sites ({@code
 * CreateIngestionExecutionRequestResolver}, {@code CreateTestConnectionRequestResolver}, {@code
 * IngestionScheduler}). Callers pass their own SLF4J logger so each entry is tagged with the
 * caller's class — operators' existing class-name log filters keep working.
 *
 * <p>The resolution ladder spans four tiers and three trigger paths. Producing a parallel-shaped
 * line at every call site lets {@code grep "Resolved ingestion CLI version"} surface every
 * resolution in the same shape, with the trigger label distinguishing the source.
 */
public final class IngestionCliVersionResolutionLogger {

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

  private IngestionCliVersionResolutionLogger() {}

  /**
   * Emit a DEBUG line capturing which tier of the resolution ladder produced the version, and a
   * WARN line if the resolved version is empty — empty means every tier (including {@code
   * defaultCliVersion}) fell through and the executor will silently use its bundled CLI.
   *
   * @param log SLF4J logger to use. Pass the caller's {@code @Slf4j}-generated {@code log} so log
   *     lines appear under the caller's class.
   * @param trigger short label distinguishing the call site (see {@link #TRIGGER_MANUAL}, etc.)
   * @param resolution result from {@link IngestionCliVersionResolutionHelper#resolve(String,
   *     String, IngestionCliVersionMatrixService, String)}
   * @param connectorType source type from the recipe, may be {@code null} when the recipe lacks a
   *     parseable {@code source.type}
   * @param identifierKey self-describing key for {@code identifierValue} — typically {@link
   *     #IDENTIFIER_INGESTION_SOURCE} or {@link #IDENTIFIER_EXECUTION_REQUEST}
   * @param identifierValue the URN of the ingestion source or execution request being logged
   */
  public static void log(
      final Logger log,
      final String trigger,
      final IngestionCliVersionResolutionHelper.Result resolution,
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
