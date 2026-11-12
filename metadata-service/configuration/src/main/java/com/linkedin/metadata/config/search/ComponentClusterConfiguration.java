package com.linkedin.metadata.config.search;

import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Maps each search subsystem to a cluster name declared under {@code elasticsearch.clusters}.
 *
 * <p>Every component defaults to {@code primary}, so an existing single-cluster deployment behaves
 * exactly as before and no component silently starts talking to a second cluster.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class ComponentClusterConfiguration {
  public static final String DEFAULT_CLUSTER = "primary";

  private String searchV2;
  private String searchV3;
  private String semantic;
  private String graph;
  private String timeseries;
  private String systemMetadata;
  private String usage;

  /** Cluster name serving the given component, falling back to {@code primary} when unset. */
  @Nonnull
  public String clusterFor(@Nonnull SearchComponent component) {
    final String configured;
    switch (component) {
      case SEARCH_V2:
        configured = searchV2;
        break;
      case SEARCH_V3:
        configured = searchV3;
        break;
      case SEMANTIC:
        configured = semantic;
        break;
      case GRAPH:
        configured = graph;
        break;
      case TIMESERIES:
        configured = timeseries;
        break;
      case SYSTEM_METADATA:
        configured = systemMetadata;
        break;
      case USAGE:
        configured = usage;
        break;
      default:
        throw new IllegalArgumentException("Unhandled search component: " + component);
    }
    return configured == null || configured.trim().isEmpty() ? DEFAULT_CLUSTER : configured.trim();
  }
}
