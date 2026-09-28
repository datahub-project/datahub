package com.linkedin.metadata.ingestion;

import java.util.Collections;
import java.util.Set;

/**
 * A canary cohort within a {@link ConnectorEntry}: a CLI version that should be served to a
 * specific allowlist of deployments instead of the connector's default.
 *
 * <p>Used for staged rollouts — pin a fix to a few deployments first, validate, then widen the
 * allowlist.
 */
public final class Cohort {

  private final String version;
  private final Set<String> deployments;

  public Cohort(String version, Set<String> deployments) {
    this.version = version;
    this.deployments =
        deployments == null ? Collections.emptySet() : Collections.unmodifiableSet(deployments);
  }

  public String getVersion() {
    return version;
  }

  /** Deployment identifiers that should receive this cohort's version. */
  public Set<String> getDeployments() {
    return deployments;
  }
}
