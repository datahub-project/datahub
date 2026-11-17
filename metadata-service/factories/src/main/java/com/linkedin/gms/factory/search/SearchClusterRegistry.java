package com.linkedin.gms.factory.search;

import com.linkedin.metadata.config.search.ClusterMigrationConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.FamilyMigrationSettings;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.SearchWriteAccess;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import com.linkedin.metadata.search.elasticsearch.update.MirroredESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.MirroredWriteLog;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Resolves which search cluster serves each subsystem, and holds the per-cluster client, bulk
 * processor and index builder.
 *
 * <p>Callers ask for a component ({@code clientFor(SEARCH_V3)}) rather than a cluster name, so the
 * routing table stays a deployment concern. When every component points at {@code primary} — the
 * default — every lookup returns the same objects and behavior is identical to a single-cluster
 * deployment.
 */
public class SearchClusterRegistry implements SearchClusterAccess, SearchWriteAccess {

  /** Everything bound to one named cluster. */
  @Value
  public static class ClusterConnection {
    String name;
    ElasticSearchConfiguration config;
    SearchClientShim<?> client;
    ESBulkProcessor bulkProcessor;
    ESIndexBuilder indexBuilder;

    /** View of this cluster as the process-wide ES components bundle upgrade steps expect. */
    @Nonnull
    public BaseElasticSearchComponentsFactory.BaseElasticSearchComponents asComponents(
        @Nonnull IndexConvention indexConvention) {
      return new BaseElasticSearchComponentsFactory.BaseElasticSearchComponents(
          config, client, indexConvention, bulkProcessor, indexBuilder);
    }
  }

  /**
   * One non-entity family being moved to another cluster: its writes are mirrored there through
   * {@link #bulkProcessorFor(SearchComponent)}, and the upgrade steps copy its history there.
   */
  @Value
  public static class FamilyMigration {
    SearchComponent family;
    String sourceCluster;
    String targetCluster;
    FamilyMigrationSettings settings;
    MigrationIndexNames names;

    /**
     * The delete log the mirror keeps while the copy runs, so the copy can undo resurrections; null
     * for a mirror-only migration, which no copy follows.
     */
    @Nullable MirroredWriteLog writeLog;
  }

  private final Map<String, ClusterConnection> connections;
  private final ElasticSearchConfiguration configuration;
  private final List<ClusterConnection> uniqueConnections;
  private final Map<SearchComponent, FamilyMigration> migrations;
  private final Map<SearchComponent, ESBulkProcessor> migrationWriters;

  /**
   * @param indexConvention names the mirrored writes on the target cluster. Required whenever a
   *     family migration is configured; a single-cluster registry, which cannot host a migration,
   *     may pass null.
   * @param metricUtils counts mirrored writes per family; null disables the counter
   */
  public SearchClusterRegistry(
      @Nonnull ElasticSearchConfiguration configuration,
      @Nonnull Map<String, ClusterConnection> connections,
      @Nullable IndexConvention indexConvention,
      @Nullable MetricUtils metricUtils) {
    this.configuration = configuration;
    this.connections = connections;
    for (SearchComponent component : SearchComponent.values()) {
      String clusterName = clusterNameFor(component);
      if (!connections.containsKey(clusterName)) {
        throw new IllegalStateException(
            "elasticsearch.componentCluster."
                + component.getConfigKey()
                + " refers to cluster '"
                + clusterName
                + "' which has no client; configured clusters: "
                + connections.keySet());
      }
    }
    this.uniqueConnections = distinctConnections(connections);
    this.migrations = familyMigrations(configuration, connections, indexConvention);
    this.migrationWriters = mirroredWriters(migrations, connections, metricUtils);
  }

  /**
   * Resolves every configured family migration against the clusters that actually have clients. The
   * configuration already rejects a target without a uri or equal to the serving name; this catches
   * two names that share one client, which would mirror a family onto itself.
   */
  @Nonnull
  private static Map<SearchComponent, FamilyMigration> familyMigrations(
      @Nonnull ElasticSearchConfiguration configuration,
      @Nonnull Map<String, ClusterConnection> connections,
      @Nullable IndexConvention indexConvention) {
    EnumMap<SearchComponent, FamilyMigration> migrations = new EnumMap<>(SearchComponent.class);
    // Read the raw block rather than activeMigrationFor(): that normalizes the whole cluster
    // configuration, which narrow contexts (tests with a routing table only) never ask for. A
    // mocked configuration answers null here, which simply means no migration.
    ClusterMigrationConfiguration migrationConfig = configuration.getClusterMigration();
    if (migrationConfig == null) {
      return Map.of();
    }
    for (SearchComponent family : ClusterMigrationConfiguration.MIGRATABLE_FAMILIES) {
      FamilyMigrationSettings settings = migrationConfig.forComponent(family);
      if (settings == null || !settings.isConfigured()) {
        continue;
      }
      if (indexConvention == null) {
        throw new IllegalStateException(
            "elasticsearch.clusterMigration."
                + family.getConfigKey()
                + " is configured but this registry was built without an IndexConvention");
      }
      String sourceCluster = configuration.getComponentCluster().clusterFor(family);
      String targetCluster = settings.getTargetCluster();
      ClusterConnection source = connections.get(sourceCluster);
      ClusterConnection target = connections.get(targetCluster);
      String keyPrefix = "elasticsearch.clusterMigration." + family.getConfigKey();
      if (target == null) {
        throw new IllegalStateException(
            keyPrefix
                + ".targetCluster refers to cluster '"
                + targetCluster
                + "' which has no client; configured clusters: "
                + connections.keySet());
      }
      if (source != null && source.getClient() == target.getClient()) {
        throw new IllegalStateException(
            keyPrefix
                + ".targetCluster '"
                + targetCluster
                + "' resolves to the same connection as the serving cluster '"
                + sourceCluster
                + "'");
      }
      SearchClusterSettings targetSettings =
          configuration.getClusters() == null
              ? null
              : configuration.getClusters().get(targetCluster);
      String targetPrefixOverlay =
          targetSettings == null || targetSettings.getIndex() == null
              ? null
              : targetSettings.getIndex().getPrefix();
      MigrationIndexNames names =
          MigrationIndexNames.forFamily(indexConvention, family, targetPrefixOverlay);
      // Narrow contexts (tests) that register connections without processors keep no log.
      MirroredWriteLog writeLog =
          settings.isCopyEnabled() && target.getBulkProcessor() != null
              ? new MirroredWriteLog(target.getBulkProcessor(), names)
              : null;
      migrations.put(
          family,
          new FamilyMigration(family, sourceCluster, targetCluster, settings, names, writeLog));
    }
    return migrations.isEmpty() ? Map.of() : Map.copyOf(migrations);
  }

  @Nonnull
  private static Map<SearchComponent, ESBulkProcessor> mirroredWriters(
      @Nonnull Map<SearchComponent, FamilyMigration> migrations,
      @Nonnull Map<String, ClusterConnection> connections,
      @Nullable MetricUtils metricUtils) {
    if (migrations.isEmpty()) {
      return Map.of();
    }
    EnumMap<SearchComponent, ESBulkProcessor> writers = new EnumMap<>(SearchComponent.class);
    for (FamilyMigration migration : migrations.values()) {
      ESBulkProcessor serving = connections.get(migration.getSourceCluster()).getBulkProcessor();
      ESBulkProcessor target = connections.get(migration.getTargetCluster()).getBulkProcessor();
      if (serving == null || target == null) {
        // Narrow contexts (tests) that register connections without processors get no mirror.
        continue;
      }
      writers.put(
          migration.getFamily(),
          new MirroredESBulkProcessor(
              serving, target, migration.getNames(), migration.getWriteLog(), metricUtils));
    }
    return writers.isEmpty() ? Map.of() : Map.copyOf(writers);
  }

  /**
   * Registry for a deployment where one cluster serves every component. This is the default shape,
   * and it is what contexts that wire a single client — notably narrow Spring tests — need.
   */
  @Nonnull
  public static SearchClusterRegistry singleCluster(
      @Nonnull ElasticSearchConfiguration configuration,
      @Nonnull SearchClientShim<?> client,
      @Nonnull ESBulkProcessor bulkProcessor,
      @Nonnull ESIndexBuilder indexBuilder) {
    // One cluster cannot host a family migration, so no convention or metrics are needed.
    return new SearchClusterRegistry(
        configuration,
        Map.of(
            ElasticSearchConfiguration.PRIMARY_CLUSTER,
            new ClusterConnection(
                ElasticSearchConfiguration.PRIMARY_CLUSTER,
                configuration,
                client,
                bulkProcessor,
                indexBuilder)),
        null,
        null);
  }

  /** Names of all clusters that have a client, in declaration order. */
  @Nonnull
  public Set<String> clusterNames() {
    return connections.keySet();
  }

  /**
   * Distinct HTTP clusters in declaration order. Aliased names that share a client appear once
   * under the first declared name, so user/role setup and health probes are not repeated against
   * the same endpoint.
   */
  @Nonnull
  public Collection<ClusterConnection> uniqueConnections() {
    return uniqueConnections;
  }

  @Nonnull
  private static List<ClusterConnection> distinctConnections(
      @Nonnull Map<String, ClusterConnection> connections) {
    IdentityHashMap<SearchClientShim<?>, Boolean> seen = new IdentityHashMap<>();
    List<ClusterConnection> unique = new ArrayList<>();
    for (ClusterConnection connection : connections.values()) {
      if (seen.putIfAbsent(connection.getClient(), Boolean.TRUE) == null) {
        unique.add(connection);
      }
    }
    return List.copyOf(unique);
  }

  @Nonnull
  public String clusterNameFor(@Nonnull SearchComponent component) {
    return configuration.getComponentCluster().clusterFor(component);
  }

  @Nonnull
  public ClusterConnection connection(@Nonnull String clusterName) {
    ClusterConnection connection = connections.get(clusterName);
    if (connection == null) {
      throw new IllegalArgumentException(
          "No search cluster named '"
              + clusterName
              + "'; configured clusters: "
              + connections.keySet());
    }
    return connection;
  }

  @Nonnull
  public ClusterConnection connectionFor(@Nonnull SearchComponent component) {
    return connection(clusterNameFor(component));
  }

  @Nonnull
  public SearchClientShim<?> client(@Nonnull String clusterName) {
    return connection(clusterName).getClient();
  }

  @Override
  @Nonnull
  public SearchClientShim<?> clientFor(@Nonnull SearchComponent component) {
    return connectionFor(component).getClient();
  }

  /**
   * The serving cluster's processor, or, for a family in migration, a mirror that also applies the
   * write to the migration target.
   */
  @Override
  @Nonnull
  public ESBulkProcessor bulkProcessorFor(@Nonnull SearchComponent component) {
    ESBulkProcessor mirrored = migrationWriters.get(component);
    return mirrored != null ? mirrored : connectionFor(component).getBulkProcessor();
  }

  /**
   * One real processor per distinct cluster. The interface default dedups by identity, which would
   * now surface the mirror wrappers; draining the underlying processors once each is what
   * ack-after-transfer needs.
   */
  @Override
  @Nonnull
  public Collection<ESBulkProcessor> bulkProcessors() {
    List<ESBulkProcessor> processors = new ArrayList<>();
    for (ClusterConnection connection : uniqueConnections) {
      if (connection.getBulkProcessor() != null) {
        processors.add(connection.getBulkProcessor());
      }
    }
    return processors;
  }

  /** Every family currently configured to migrate, in family order. */
  @Nonnull
  public Collection<FamilyMigration> familyMigrations() {
    return migrations.values();
  }

  @Nonnull
  public Optional<FamilyMigration> migrationFor(@Nonnull SearchComponent family) {
    return Optional.ofNullable(migrations.get(family));
  }

  /** The connection a family's writes are mirrored to and its history copied to. */
  @Nonnull
  public ClusterConnection targetConnectionFor(@Nonnull SearchComponent family) {
    FamilyMigration migration =
        migrationFor(family)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "No cluster migration is configured for " + family.getConfigKey()));
    return connection(migration.getTargetCluster());
  }

  @Nonnull
  public ESIndexBuilder indexBuilderFor(@Nonnull SearchComponent component) {
    return connectionFor(component).getIndexBuilder();
  }

  /** Effective configuration for the cluster serving this component, with overlays applied. */
  @Nonnull
  public ElasticSearchConfiguration configFor(@Nonnull SearchComponent component) {
    return connectionFor(component).getConfig();
  }

  /**
   * True when two components share a cluster, in which case callers can keep using a single client
   * and skip split-cluster handling such as building two index families separately.
   */
  public boolean sameCluster(@Nonnull SearchComponent a, @Nonnull SearchComponent b) {
    return clusterNameFor(a).equals(clusterNameFor(b));
  }

  /**
   * Maps a resolved entity index name to the component that owns it.
   *
   * @see SearchClusterAccess#componentForEntityIndex
   */
  @Nonnull
  public static SearchComponent componentForEntityIndex(
      @Nonnull IndexConvention convention, @Nonnull String indexName) {
    return SearchClusterAccess.componentForEntityIndex(convention, indexName);
  }

  /**
   * Index-builder resolver for {@code ElasticSearchService}, or null when every entity search
   * component shares the primary cluster and the existing single builder is already correct.
   */
  @Nullable
  public Function<String, ESIndexBuilder> entityIndexBuilderResolver(
      @Nonnull IndexConvention convention) {
    if (sameCluster(SearchComponent.SEARCH_V2, SearchComponent.SEARCH_V3)
        && sameCluster(SearchComponent.SEARCH_V2, SearchComponent.SEMANTIC)
        && ElasticSearchConfiguration.PRIMARY_CLUSTER.equals(
            clusterNameFor(SearchComponent.SEARCH_V2))) {
      return null;
    }
    return indexName -> indexBuilderFor(componentForEntityIndex(convention, indexName));
  }
}
