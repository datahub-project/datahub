package com.linkedin.metadata.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;

@Builder
public class EntityClientAspectRetriever implements AspectRetriever {
  @Getter private final EntityRegistry entityRegistry;
  private final SystemEntityClient entityClient;

  @Nullable
  @Override
  public Aspect getLatestAspectObject(@Nonnull Urn urn, @Nonnull String aspectName)
      throws RemoteInvocationException, URISyntaxException {
    return entityClient.getLatestAspectObject(urn, aspectName);
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
      Set<Urn> urns, Set<String> aspectNames) throws RemoteInvocationException, URISyntaxException {
    return entityClient.getLatestAspects(urns, aspectNames);
  }
}
