/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.models.registry.config;

import java.util.Collections;
import java.util.Set;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
public class EntityRegistryLoadResult {
  private LoadStatus loadResult;
  private String registryLocation;
  private String failureReason;
  @Setter private int failureCount;
  private PluginLoadResult plugins;

  @Builder
  @Data
  public static class PluginLoadResult {
    private int validatorCount;
    private int mutationHookCount;
    private int mcpSideEffectCount;
    private int mclSideEffectCount;

    @Builder.Default private Set<String> validatorClasses = Collections.emptySet();
    @Builder.Default private Set<String> mutationHookClasses = Collections.emptySet();
    @Builder.Default private Set<String> mcpSideEffectClasses = Collections.emptySet();
    @Builder.Default private Set<String> mclSideEffectClasses = Collections.emptySet();
  }
}
