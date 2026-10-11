package com.linkedin.metadata.ingestion;

import java.util.Collections;
import java.util.List;

/**
 * A single {@code (serverVersion, connectorType)} entry in the matrix: a connector-level default
 * version plus an ordered list of canary cohorts.
 *
 * <p>Resolution against this entry walks the cohorts in order and returns the first one whose
 * {@code deployments} list contains the deployment's id. If no cohort matches, the {@code
 * defaultVersion} is used.
 */
public final class ConnectorEntry {

  private final String defaultVersion;
  private final List<Cohort> cohorts;

  public ConnectorEntry(String defaultVersion, List<Cohort> cohorts) {
    this.defaultVersion = defaultVersion;
    this.cohorts =
        cohorts == null ? Collections.emptyList() : Collections.unmodifiableList(cohorts);
  }

  /** The {@code _default} version applied when no cohort allowlist matches. May be {@code null}. */
  public String getDefaultVersion() {
    return defaultVersion;
  }

  /** Cohorts in declaration order. First match wins. Never {@code null}; may be empty. */
  public List<Cohort> getCohorts() {
    return cohorts;
  }
}
