package com.linkedin.datahub.upgrade.cleanup;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeStep;
import java.util.List;

/**
 * Upgrade that tears down all infrastructure resources created by DataHub setup jobs. Intended to
 * run as a Helm pre-delete hook so that {@code helm uninstall} leaves no DataHub-specific state in
 * shared infrastructure (Elasticsearch, Kafka, SQL).
 *
 * <p>Execution order: Elasticsearch → Kafka → SQL. Elasticsearch is cleaned first so that indices
 * are not queried while the database is being dropped.
 */
public class Cleanup implements Upgrade {

  private final List<UpgradeStep> steps;

  public Cleanup(List<UpgradeStep> steps) {
    this.steps = steps;
  }

  @Override
  public String id() {
    return "Cleanup";
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
