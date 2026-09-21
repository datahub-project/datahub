package com.linkedin.metadata.config.search;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One named search cluster ({@code elasticsearch.clusters.<name>}).
 *
 * <p>A cluster owns everything that identifies or authenticates a connection plus its own sizing.
 * Shared operational tuning (bulk processing, index build behavior, ingest caps) lives at {@code
 * elasticsearch.*} and is inherited; the {@code bulkProcessor} / {@code bulkDelete} / {@code
 * buildIndices} maps here are sparse field-level overlays for the rare case where one cluster needs
 * to differ.
 *
 * <p>Nothing is ever inherited from another cluster. In particular a blank {@link #uri} means "not
 * configured" rather than "same as primary" — silently aliasing two cluster names onto one
 * connection would make a misconfigured routing table look like it worked.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class SearchClusterSettings {
  /** {@code scheme://host:port[/pathPrefix]}. Blank means this cluster is not configured. */
  private String uri;

  private String username;
  private String password;
  private boolean opensearchUseAwsIamAuth;
  private String region;

  /**
   * Sizing for this cluster; also the default shard count when {@code index.numShards} is unset.
   */
  private Integer dataNodeCount;

  /**
   * Optional per-cluster RestClient thread count; unset inherits {@code elasticsearch.threadCount}.
   */
  private Integer threadCount;

  private SearchClusterIndexSettings index;
  private ShimSettings shim;
  private SslContextSettings sslContext;

  /**
   * Sparse overlays on the shared {@code elasticsearch.*} operational blocks. Kept as raw maps so a
   * deployment can override a single key without restating the whole block.
   */
  private Map<String, Object> bulkProcessor;

  private Map<String, Object> bulkDelete;
  private Map<String, Object> buildIndices;

  /** True when no endpoint is configured, meaning no client should be created for this cluster. */
  public boolean isConfigured() {
    return uri != null && !uri.trim().isEmpty();
  }

  /**
   * Unset credentials must read as null, not as the empty string: the clients only attach basic
   * auth when both username and password are non-null, so an empty string would make every request
   * to an unauthenticated cluster carry a useless Authorization header.
   */
  public String getUsername() {
    return blankToNull(username);
  }

  public String getPassword() {
    return blankToNull(password);
  }

  public String getRegion() {
    return blankToNull(region);
  }

  public static String blankToNull(String value) {
    return value == null || value.trim().isEmpty() ? null : value;
  }

  /**
   * Parses {@link #uri}, which callers must only do after checking {@link #isConfigured()}.
   *
   * @param clusterName used to make parse failures identifiable
   */
  @Nonnull
  public SearchClusterUri parsedUri(@Nonnull String clusterName) {
    if (!isConfigured()) {
      throw new IllegalStateException(
          "elasticsearch.clusters." + clusterName + ".uri is not configured");
    }
    return SearchClusterUri.parse(clusterName, uri);
  }

  /**
   * Effective shard count: an explicit override, otherwise this cluster's own node count, otherwise
   * a single shard.
   */
  public int effectiveNumShards() {
    if (index != null && index.getNumShards() != null) {
      return index.getNumShards();
    }
    return dataNodeCount == null ? 1 : dataNodeCount;
  }

  /** Effective replica count, defaulting to one replica. */
  public int effectiveNumReplicas() {
    if (index != null && index.getNumReplicas() != null) {
      return index.getNumReplicas();
    }
    return 1;
  }

  /** Effective index configuration: shared defaults with this cluster's overlay applied. */
  @Nonnull
  public IndexConfiguration effectiveIndex(@Nullable IndexConfiguration defaults) {
    IndexConfiguration merged =
        index == null
            ? (defaults == null
                ? IndexConfiguration.builder().build()
                : defaults.toBuilder().build())
            : index.applyTo(defaults);
    merged.setNumShards(effectiveNumShards());
    merged.setNumReplicas(effectiveNumReplicas());
    return merged;
  }
}
