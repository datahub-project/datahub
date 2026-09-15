package com.linkedin.gms.factory.search;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.SearchWriteAccess;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
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

  private final Map<String, ClusterConnection> connections;
  private final ElasticSearchConfiguration configuration;

  public SearchClusterRegistry(
      @Nonnull ElasticSearchConfiguration configuration,
      @Nonnull Map<String, ClusterConnection> connections) {
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
    return new SearchClusterRegistry(
        configuration,
        Map.of(
            ElasticSearchConfiguration.PRIMARY_CLUSTER,
            new ClusterConnection(
                ElasticSearchConfiguration.PRIMARY_CLUSTER,
                configuration,
                client,
                bulkProcessor,
                indexBuilder)));
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

  @Override
  @Nonnull
  public ESBulkProcessor bulkProcessorFor(@Nonnull SearchComponent component) {
    return connectionFor(component).getBulkProcessor();
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
