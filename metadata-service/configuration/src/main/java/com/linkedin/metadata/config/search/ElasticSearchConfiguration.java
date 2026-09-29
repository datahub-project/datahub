package com.linkedin.metadata.config.search;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class ElasticSearchConfiguration {
  public static final String PRIMARY_CLUSTER = ComponentClusterConfiguration.DEFAULT_CLUSTER;

  /**
   * Shared operational defaults. Every cluster inherits these; a cluster may overlay individual
   * fields via {@code clusters.<name>.bulkProcessor} and friends. These are deliberately not hung
   * off {@code clusters.primary}: primary is a real cluster, not a defaults bucket.
   */
  private BulkDeleteConfiguration bulkDelete;

  private BulkProcessorConfiguration bulkProcessor;
  private BuildIndicesConfiguration buildIndices;
  private SearchConfiguration search;
  private IndexConfiguration index;
  private ScrollConfiguration scroll;
  private EntityIndexConfiguration entityIndex;

  private int threadCount;
  private int connectionRequestTimeout;
  private int socketTimeout;

  /** Named clusters. {@code primary} is required; additional names are optional. */
  private Map<String, SearchClusterSettings> clusters;

  /** Which cluster serves each subsystem. */
  private ComponentClusterConfiguration componentCluster;

  /**
   * Legacy connection inputs, still bound from {@code ELASTICSEARCH_HOST} / {@code PORT} / {@code
   * USE_SSL} / {@code PATH_PREFIX} so existing Compose and Cloud deployments keep working without
   * setting {@code ELASTICSEARCH_URI}. They are inputs only — read the effective values through
   * {@link #getHost()} and friends, which resolve from the primary cluster URI.
   */
  @Getter(AccessLevel.NONE)
  private String host;

  @Getter(AccessLevel.NONE)
  private Integer port;

  @Getter(AccessLevel.NONE)
  private Boolean useSSL;

  @Getter(AccessLevel.NONE)
  private String pathPrefix;

  @Getter(AccessLevel.NONE)
  private String username;

  @Getter(AccessLevel.NONE)
  private String password;

  @Getter(AccessLevel.NONE)
  private String region;

  @Getter(AccessLevel.NONE)
  private Boolean opensearchUseAwsIamAuth;

  /** Guards one-time synthesis of the primary URI from the legacy inputs. */
  @Getter(AccessLevel.NONE)
  private transient volatile boolean clustersNormalized;

  /**
   * Resolves {@code clusters.primary.uri} from the legacy host/port inputs when it was not set
   * directly, and validates the routing table. Idempotent, and safe to call from any accessor.
   */
  public synchronized void normalizeClusters() {
    if (clustersNormalized) {
      return;
    }
    // Copy defensively: callers (and tests) may hand us an immutable map, and we add a primary
    // entry and rewrite its uri below.
    clusters = clusters == null ? new LinkedHashMap<>() : new LinkedHashMap<>(clusters);
    SearchClusterSettings primary =
        clusters.computeIfAbsent(PRIMARY_CLUSTER, k -> SearchClusterSettings.builder().build());

    boolean legacyProvided =
        (host != null && !host.trim().isEmpty())
            || port != null
            || useSSL != null
            || (pathPrefix != null && !pathPrefix.trim().isEmpty());

    if (!primary.isConfigured()) {
      primary.setUri(
          SearchClusterUri.synthesize(
              host, port == null ? 9200 : port, Boolean.TRUE.equals(useSSL), pathPrefix));
      log.info(
          "elasticsearch.clusters.primary.uri synthesized from legacy host/port settings: {}",
          primary.getUri());
    } else if (legacyProvided) {
      // Compose images often still export ELASTICSEARCH_HOST alongside a newer URI. Preferring the
      // explicit URI and saying so is friendlier than failing a deployment that set both.
      log.warn(
          "elasticsearch.clusters.primary.uri is set ({}); ignoring legacy ELASTICSEARCH_HOST/PORT/USE_SSL/PATH_PREFIX",
          primary.getUri());
    }

    // Legacy top-level credentials apply to the primary cluster unless it declares its own.
    if (primary.getUsername() == null) {
      primary.setUsername(SearchClusterSettings.blankToNull(username));
    }
    if (primary.getPassword() == null) {
      primary.setPassword(SearchClusterSettings.blankToNull(password));
    }
    if (primary.getRegion() == null) {
      primary.setRegion(SearchClusterSettings.blankToNull(region));
    }
    if (Boolean.TRUE.equals(opensearchUseAwsIamAuth)) {
      primary.setOpensearchUseAwsIamAuth(true);
    }

    // The V2 tokenizer is a product default under entityIndex.v2, but the settings builders read
    // it off the effective IndexConfiguration. Fold it in so a cluster overlay can still win.
    if (index != null
        && (index.getMainTokenizer() == null || index.getMainTokenizer().isEmpty())
        && entityIndex != null
        && entityIndex.getV2() != null) {
      index.setMainTokenizer(entityIndex.getV2().getMainTokenizer());
    }

    // Fail fast on a routing table that points at a cluster nobody configured, rather than at
    // first query.
    for (SearchComponent component : SearchComponent.values()) {
      String clusterName = getComponentCluster().clusterFor(component);
      SearchClusterSettings target = clusters.get(clusterName);
      if (target == null || !target.isConfigured()) {
        throw new IllegalStateException(
            "elasticsearch.componentCluster."
                + component.getConfigKey()
                + " refers to cluster '"
                + clusterName
                + "' which has no uri configured");
      }
    }
    clustersNormalized = true;
  }

  /**
   * All clusters, with the primary URI resolved and the routing table validated.
   *
   * <p>Deliberately not the {@code getClusters()} JavaBean getter: Spring's binder calls that
   * getter while it is still populating the object, and normalizing at that point would fix up a
   * half-bound configuration and then be discarded by the binder's own setter.
   */
  @Nonnull
  public Map<String, SearchClusterSettings> resolvedClusters() {
    normalizeClusters();
    return clusters;
  }

  public void setClusters(Map<String, SearchClusterSettings> clusters) {
    this.clusters = clusters;
    this.clustersNormalized = false;
  }

  @Nonnull
  public ComponentClusterConfiguration getComponentCluster() {
    if (componentCluster == null) {
      componentCluster = ComponentClusterConfiguration.builder().build();
    }
    return componentCluster;
  }

  /** Settings for a named cluster. */
  @Nonnull
  public SearchClusterSettings getCluster(@Nonnull String name) {
    SearchClusterSettings settings = resolvedClusters().get(name);
    if (settings == null) {
      throw new IllegalArgumentException("No elasticsearch.clusters entry named '" + name + "'");
    }
    return settings;
  }

  /** Settings for the cluster serving the given component. */
  @Nonnull
  public SearchClusterSettings getCluster(@Nonnull SearchComponent component) {
    return getCluster(getComponentCluster().clusterFor(component));
  }

  @Nonnull
  public SearchClusterSettings getPrimaryCluster() {
    return getCluster(PRIMARY_CLUSTER);
  }

  private SearchClusterUri primaryUri() {
    return getPrimaryCluster().parsedUri(PRIMARY_CLUSTER);
  }

  public String getHost() {
    return primaryUri().getHost();
  }

  public int getPort() {
    return primaryUri().getPort();
  }

  public boolean isUseSSL() {
    return primaryUri().isUseSSL();
  }

  public String getPathPrefix() {
    return primaryUri().getPathPrefix();
  }

  public String getUsername() {
    return getPrimaryCluster().getUsername();
  }

  public String getPassword() {
    return getPrimaryCluster().getPassword();
  }

  public boolean isOpensearchUseAwsIamAuth() {
    return getPrimaryCluster().isOpensearchUseAwsIamAuth();
  }

  public String getRegion() {
    return getPrimaryCluster().getRegion();
  }

  /** Search V2 document id hashing. Search V3 always hashes and has no equivalent knob. */
  public String getIdHashAlgo() {
    return entityIndex == null || entityIndex.getV2() == null
        ? null
        : entityIndex.getV2().getIdHashAlgo();
  }
}
