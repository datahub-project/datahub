package com.linkedin.metadata.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Getter
@Builder
@Component
@RequiredArgsConstructor
public class EntityClientAspectRetriever implements CachingAspectRetriever {
  private final EntityRegistry entityRegistry;
  private final SystemEntityClient entityClient;

  /**
   * Preventing a circular dependency. Once constructed the AspectRetriever is injected into a few
   * of the services which rely on the AspectRetriever when using the Java EntityClient. The Java
   * EntityClient depends on services which in turn depend on the AspectRetriever
   */
  @PostConstruct
  public void postConstruct() {
    entityClient.postConstruct(this);
  }

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
    if (urns.isEmpty() || aspectNames.isEmpty()) {
      return Map.of();
    } else {
      return entityClient.getLatestAspects(urns, aspectNames);
    }
  }
}
