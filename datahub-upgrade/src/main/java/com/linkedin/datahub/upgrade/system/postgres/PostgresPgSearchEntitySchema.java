package com.linkedin.datahub.upgrade.system.postgres;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.ebean.Database;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Blocking system upgrade for pgSearch: seeds {@code group_registry} from {@link EntityRegistry}
 * (Elasticsearch v3 search-group parity). DDL runs in {@link
 * com.linkedin.datahub.upgrade.sqlsetup.postgres.PgSearchEntitySchemaStep} during SqlSetup.
 *
 * <p>Gated by {@code postgres.pgSearch.entity.enabled}. Idempotent with SqlSetup when both run.
 *
 * <p>See this package's {@code package-info.java} for when work belongs here vs {@code
 * com.linkedin.datahub.upgrade.sqlsetup.postgres}.
 */
public class PostgresPgSearchEntitySchema implements BlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public PostgresPgSearchEntitySchema(
      @Nonnull Database ebeanServer,
      @Nonnull PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Nonnull EntityRegistry entityRegistry) {
    this.steps =
        ImmutableList.of(
            new PgSearchEntitySearchGroupRegistrySeedStep(
                ebeanServer, postgresSqlSetupProperties, entityRegistry));
  }

  @Override
  public String id() {
    return "PostgresPgSearchEntitySchema";
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
