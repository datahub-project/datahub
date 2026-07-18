package com.linkedin.datahub.upgrade.system.postgres;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupSession;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Inserts {@code {prefix}_group_registry} rows from {@link EntityRegistry#getSearchGroups()}
 * (Elasticsearch v3 index group parity), plus {@link EntityAnnotation#DEFAULT_SEARCH_GROUP}.
 *
 * <p>Runs after {@link com.linkedin.datahub.upgrade.sqlsetup.postgres.PgSearchEntitySchemaStep} DDL
 * in blocking {@code SystemUpdate}. Idempotent ({@code ON CONFLICT DO NOTHING}).
 */
@Slf4j
@RequiredArgsConstructor
public class PgSearchEntitySearchGroupRegistrySeedStep implements UpgradeStep {

  private final Database server;
  private final PostgresSqlSetupProperties postgresProperties;
  @Nonnull private final EntityRegistry entityRegistry;

  @Override
  public String id() {
    return "PgSearchEntitySearchGroupRegistrySeedStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Seeding pgSearch group_registry from entity registry...");
        try (Connection connection = server.dataSource().getConnection()) {
          connection.setAutoCommit(true);
          String schema = postgresProperties.normalizedPostgresSchema();
          PostgresSqlSetupSession.ensureSchemaAndSearchPath(connection, schema);
          String tablePrefix = postgresProperties.normalizedPgSearchEntityTablePrefix();
          ensureSearchGroupRegistryRows(connection, tablePrefix, entityRegistry);
          context
              .report()
              .addLine(
                  "Seeded pgSearch group_registry for: "
                      + String.join(", ", resolveSearchGroups(entityRegistry)));
        }
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("PgSearchEntitySearchGroupRegistrySeedStep failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /**
   * Search groups that must exist in {@code {prefix}_group_registry} before dual-write. Mirrors
   * Elasticsearch v3's per-group indices ({@link EntityRegistry#getSearchGroups()}).
   */
  @Nonnull
  static LinkedHashSet<String> resolveSearchGroups(@Nonnull EntityRegistry entityRegistry) {
    LinkedHashSet<String> groups = new LinkedHashSet<>();
    groups.add(EntityAnnotation.DEFAULT_SEARCH_GROUP);
    entityRegistry.getSearchGroups().stream()
        .filter(Objects::nonNull)
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .forEach(groups::add);
    return groups;
  }

  private static void ensureSearchGroupRegistryRows(
      Connection connection, String tablePrefix, EntityRegistry entityRegistry)
      throws SQLException {
    String registryTable = tablePrefix + "_group_registry";
    String physicalTable = tablePrefix + "_search_row";
    LinkedHashSet<String> groups = resolveSearchGroups(entityRegistry);
    String insertSql =
        "INSERT INTO "
            + registryTable
            + " (search_group, physical_table_name) VALUES (?, ?) ON CONFLICT (search_group) DO NOTHING";
    for (String group : groups) {
      try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
        ps.setString(1, group);
        ps.setString(2, physicalTable);
        ps.executeUpdate();
      }
    }
    log.info("pgSearch entity group_registry seeded for {} group(s): {}", groups.size(), groups);
  }
}
