package com.linkedin.metadata.entity;

import static com.linkedin.metadata.utils.GenericRecordUtils.entityResponseToAspectMap;
import static com.linkedin.metadata.utils.GenericRecordUtils.entityResponseToSystemAspectMap;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
public class EntityServiceAspectRetriever implements AspectRetriever {

  @Setter private OperationContext systemOperationContext;
  private final EntityRegistry entityRegistry;
  private final EntityService<?> entityService;

  @Nonnull
  @Override
  public Map<Urn, Map<String, Aspect>> getLatestAspectObjects(
      @Nonnull OperationFingerprint context, Set<Urn> urns, Set<String> aspectNames) {
    if (urns.isEmpty() || aspectNames.isEmpty()) {
      return Map.of();
    }
    String entityName = urns.stream().findFirst().map(Urn::getEntityType).get();
    try {
      // EntityService still takes the full OperationContext; cast through. In practice every
      // OperationFingerprint passed here IS an OperationContext (the OSS class implements the
      // view), so the cast is safe. Bootstrap callers that pass a non-OpContext view fall back
      // to the system context.
      OperationContext op =
          (context instanceof OperationContext)
              ? (OperationContext) context
              : Objects.requireNonNull(
                  systemOperationContext, "systemOperationContext not initialized");
      return entityResponseToAspectMap(
          entityService.getEntitiesV2(op, entityName, urns, aspectNames, false));
    } catch (URISyntaxException e) {
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
    String entityName = urnAspectNames.keySet().stream().findFirst().map(Urn::getEntityType).get();
    try {
      OperationContext op =
          (context instanceof OperationContext)
              ? (OperationContext) context
              : Objects.requireNonNull(
                  systemOperationContext, "systemOperationContext not initialized");
      // TODO - This causes over-fetching if not all aspects are required for every URN
      return entityResponseToSystemAspectMap(
          entityService.getEntitiesV2(
              op,
              entityName,
              urnAspectNames.keySet(),
              urnAspectNames.values().stream()
                  .flatMap(Collection::stream)
                  .collect(Collectors.toSet()),
              false),
          entityRegistry);
    } catch (URISyntaxException e) {
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
    final Set<Urn> existing = entityService.exists(op, urns);
    return urns.stream().collect(Collectors.toMap(urn -> urn, existing::contains));
  }
}
