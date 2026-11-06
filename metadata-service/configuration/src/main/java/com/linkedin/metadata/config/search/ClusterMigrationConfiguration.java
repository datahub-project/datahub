package com.linkedin.metadata.config.search;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * {@code elasticsearch.clusterMigration}: one optional migration per non-entity index family. The
 * entity families (Search V2, V3, semantic) move between clusters through the Search V3 fill and
 * are not covered here.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class ClusterMigrationConfiguration {
  public static final String DEFAULT_PROCESS_ROLE = "gms";

  /** Families this block can migrate, in the order the upgrade steps visit them. */
  public static final Set<SearchComponent> MIGRATABLE_FAMILIES =
      EnumSet.of(
          SearchComponent.GRAPH,
          SearchComponent.SYSTEM_METADATA,
          SearchComponent.TIMESERIES,
          SearchComponent.USAGE);

  /**
   * Which process this configuration is running in: {@code gms}, {@code mae} or {@code upgrade}.
   * The mirror-live marker records it, and the copy waits for the two live writers (GMS and the MAE
   * consumer) before reading the source. Each application's profile yaml sets its own value.
   */
  private String processRole;

  /**
   * How many minutes the non-blocking copy waits for GMS and the MAE consumer to record their
   * mirror markers before it reads the source. They record them within a minute of starting, so a
   * wait that spans the rollout lets the non-blocking run of the migrating deploy carry a family to
   * verified on its own; 0 returns at once and leaves the copy to a later run.
   */
  private int liveWriterWaitMinutes;

  private FamilyMigrationSettings graph;
  private FamilyMigrationSettings timeseries;
  private FamilyMigrationSettings systemMetadata;
  private FamilyMigrationSettings usage;

  @Nonnull
  public String getProcessRole() {
    return processRole == null || processRole.trim().isEmpty()
        ? DEFAULT_PROCESS_ROLE
        : processRole.trim().toLowerCase(Locale.ROOT);
  }

  /** The family's settings, or null for a component this block does not migrate. */
  @Nullable
  public FamilyMigrationSettings forComponent(@Nonnull final SearchComponent component) {
    switch (component) {
      case GRAPH:
        return graph;
      case TIMESERIES:
        return timeseries;
      case SYSTEM_METADATA:
        return systemMetadata;
      case USAGE:
        return usage;
      default:
        return null;
    }
  }

  /** True when the family names a target cluster. */
  public boolean isConfiguredFor(@Nonnull final SearchComponent component) {
    final FamilyMigrationSettings settings = forComponent(component);
    return settings != null && settings.isConfigured();
  }
}
