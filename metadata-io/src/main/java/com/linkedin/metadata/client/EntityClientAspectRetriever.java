package com.linkedin.metadata.client;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
public class EntityClientAspectRetriever implements CachingAspectRetriever, AspectRetriever {

  @Setter private OperationContext systemOperationContext;
  private final SystemEntityClient entityClient;

  @Nonnull
  @Override
  public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
      @Nonnull OperationFingerprint context, Set<Urn> urns, Set<String> aspectNames) {
    if (urns.isEmpty() || aspectNames.isEmpty()) {
      return Map.of();
    }
    try {
      OperationContext op =
          (context instanceof OperationContext)
              ? (OperationContext) context
              : Objects.requireNonNull(
                  systemOperationContext, "systemOperationContext not initialized");
      return entityClient.getLatestAspects(op, urns, aspectNames, false);
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
      @Nonnull OperationFingerprint context, Map<Urn, Set<String>> urnAspectNames) {
    if (urnAspectNames.isEmpty()) {
      return Map.of();
    }
    try {
      OperationContext op =
          (context instanceof OperationContext)
              ? (OperationContext) context
              : Objects.requireNonNull(
                  systemOperationContext, "systemOperationContext not initialized");
      // TODO: This generates over-fetching if not all aspects are needed for each URN
      return entityClient.getLatestSystemAspect(
          op,
          urnAspectNames.keySet(),
          urnAspectNames.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()),
          false);
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  @Override
  public Map<Urn, Boolean> entityExists(@Nonnull OperationFingerprint context, Set<Urn> urns) {
    if (urns.isEmpty()) {
      return Map.of();
    }
    OperationContext op =
        (context instanceof OperationContext)
            ? (OperationContext) context
            : Objects.requireNonNull(
                systemOperationContext, "systemOperationContext not initialized");
    try {
      final Set<Urn> existing = entityClient.filterExistingUrns(op, urns);
      return urns.stream().collect(Collectors.toMap(urn -> urn, existing::contains));
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  @Override
  public EntityRegistry getEntityRegistry() {
    return systemOperationContext.getEntityRegistry();
  }
}
