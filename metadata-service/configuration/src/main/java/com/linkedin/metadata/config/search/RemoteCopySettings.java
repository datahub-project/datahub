package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Source-side details a {@link CopyMode#REMOTE_REINDEX} copy puts in the {@code source.remote}
 * block. The target cluster's nodes make that fetch, so these are the only credentials it can
 * carry; unset username and password fall back to the serving cluster's basic-auth credentials.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class RemoteCopySettings {
  /**
   * The source as the target cluster's nodes reach it ({@code scheme://host:port}), when that is
   * not the serving cluster's own uri: an AWS VPC endpoint connection address, or a network alias
   * between containers. Unset means the serving cluster's uri.
   */
  private String host;

  private String username;
  private String password;

  /**
   * AWS OpenSearch Service requires {@code external: true} between two VPC domains or for a
   * non-Service source. Self-managed engines reject the key as unknown, so it is off by default.
   */
  private boolean external;

  private String socketTimeout;
  private String connectTimeout;

  public String getHost() {
    return SearchClusterSettings.blankToNull(host);
  }

  public String getUsername() {
    return SearchClusterSettings.blankToNull(username);
  }

  public String getPassword() {
    return SearchClusterSettings.blankToNull(password);
  }
}
