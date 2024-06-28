package com.linkedin.metadata.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
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
      return entityClient.getLatestAspectObject(systemOperationContext, urn, aspectName);
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
        return entityClient.getLatestAspects(systemOperationContext, urns, aspectNames);
      } catch (RemoteInvocationException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
