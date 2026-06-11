package com.linkedin.metadata.ingestion;

import java.util.Collections;
import java.util.Map;

/**
 * In-memory snapshot of the per-connector ingestion CLI version matrix.
 *
 * <p>The matrix is keyed by server release version → {@link ServerEntry}, which in turn maps
 * connector type to a {@link ConnectorEntry} carrying a {@code _default} version and an optional
 * ordered list of canary cohorts.
 *
 * <p>A pure POJO: {@link IngestionCliVersionMatrixSource} implementations produce it and {@link
 * IngestionCliVersionMatrixService} consumes it. The storage layer (HTTP, GMS aspect, config
 * server, …) is decoupled from the resolution layer that walks the matrix and applies precedence
 * rules.
 */
public final class IngestionCliVersionMatrix {

  /** Empty matrix used when no source is configured or fetch has not yet succeeded. */
  public static final IngestionCliVersionMatrix EMPTY =
      new IngestionCliVersionMatrix(Collections.emptyMap());

  private final Map<String, ServerEntry> entriesByServerVersion;

  public IngestionCliVersionMatrix(Map<String, ServerEntry> entriesByServerVersion) {
    this.entriesByServerVersion =
        entriesByServerVersion == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(entriesByServerVersion);
  }

  /**
   * Lookup the per-connector entries for a given server release. Returns {@code null} if the server
   * version has no entry — callers fall back to the application default.
   */
  public ServerEntry getEntriesForServer(String serverVersion) {
    return entriesByServerVersion.get(serverVersion);
  }

  /** Number of server-version keys in the matrix. Used for diagnostic logging. */
  public int size() {
    return entriesByServerVersion.size();
  }
}
