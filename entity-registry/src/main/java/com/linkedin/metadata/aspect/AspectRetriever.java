package com.linkedin.metadata.aspect;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface AspectRetriever {

  @Nullable
  default Aspect getLatestAspectObject(@Nonnull final Urn urn, @Nonnull final String aspectName)
      throws RemoteInvocationException, URISyntaxException {
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
  Map<Urn, Map<String, Aspect>> getLatestAspectObjects(Set<Urn> urns, Set<String> aspectNames)
      throws RemoteInvocationException, URISyntaxException;

  @Nonnull
  default Map<Urn, Boolean> entityExists(Set<Urn> urns) {
    Set<String> keyAspectNames =
        urns.stream()
            .map(Urn::getEntityType)
            .distinct()
            .map(entityType -> getEntityRegistry().getEntitySpec(entityType).getKeyAspectName())
            .collect(Collectors.toSet());

    try {
      Map<Urn, Map<String, Aspect>> latest = getLatestAspectObjects(urns, keyAspectNames);
      return urns.stream()
          .map(urn -> Pair.of(urn, latest.containsKey(urn)))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  EntityRegistry getEntityRegistry();
}
