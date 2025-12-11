/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface AspectRetriever {

  @Nullable
  default Aspect getLatestAspectObject(@Nonnull final Urn urn, @Nonnull final String aspectName) {
    return getLatestAspectObjects(ImmutableSet.of(urn), ImmutableSet.of(aspectName))
        .getOrDefault(urn, Collections.emptyMap())
        .get(aspectName);
  }

  /**
   * Returns for each URN, the map of aspectName to Aspect
   *
   * @param urns urns to fetch
   * @param aspectNames aspect names
   * @return urn to aspect name and values
   */
  @Nonnull
  Map<Urn, Map<String, Aspect>> getLatestAspectObjects(Set<Urn> urns, Set<String> aspectNames);

  @Nullable
  default SystemAspect getLatestSystemAspect(
      @Nonnull final Urn urn, @Nonnull final String aspectName) {
    return getLatestSystemAspects(ImmutableMap.of(urn, ImmutableSet.of(aspectName)))
        .getOrDefault(urn, Collections.emptyMap())
        .get(aspectName);
  }

  /**
   * Returns for each URN, the map of aspectName to Aspect
   *
   * @param urnAspectNames urns and aspect names to fetch
   * @return urn to aspect name and values
   */
  @Nonnull
  Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(Map<Urn, Set<String>> urnAspectNames);

  @Nonnull
  Map<Urn, Boolean> entityExists(Set<Urn> urns);

  @Nonnull
  EntityRegistry getEntityRegistry();
}
