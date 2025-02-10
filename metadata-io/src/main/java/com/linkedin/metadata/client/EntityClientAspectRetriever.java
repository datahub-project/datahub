package com.linkedin.metadata.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
public class EntityClientAspectRetriever implements CachingAspectRetriever {
  @Setter private OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;

  @Nonnull
  @Override
  public EntityRegistry getEntityRegistry() {
    return systemOperationContext.getEntityRegistry();
  }

  @Nullable
  @Override
  public Aspect getLatestAspectObject(@Nonnull Urn urn, @Nonnull String aspectName) {
    try {
      return entityClient.getLatestAspectObject(systemOperationContext, urn, aspectName, false);
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
      Set<Urn> urns, Set<String> aspectNames) {
    if (urns.isEmpty() || aspectNames.isEmpty()) {
      return Map.of();
    } else {
      try {
        return entityClient.getLatestAspects(systemOperationContext, urns, aspectNames, false);
      } catch (RemoteInvocationException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Nonnull
  public Map<Urn, Boolean> entityExists(Set<Urn> urns) {
    if (urns.isEmpty()) {
      return Map.of();
    } else {
      return urns.stream()
          .collect(
              Collectors.toMap(
                  urn -> urn,
                  urn -> {
                    try {
                      return entityClient.exists(systemOperationContext, urn);
                    } catch (RemoteInvocationException e) {
                      throw new RuntimeException(e);
                    }
                  }));
    }
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
      Map<Urn, Set<String>> urnAspectNames) {
    if (urnAspectNames.isEmpty()) {
      return Map.of();
    } else {
      try {
        // TODO: This generates over-fetching if not all aspects are needed for each URN
        return entityClient.getLatestSystemAspect(
            systemOperationContext,
            urnAspectNames.keySet(),
            urnAspectNames.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet()),
            false);
      } catch (RemoteInvocationException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
