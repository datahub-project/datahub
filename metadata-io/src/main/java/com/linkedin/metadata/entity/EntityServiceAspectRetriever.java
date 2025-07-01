package com.linkedin.metadata.entity;

import static com.linkedin.metadata.utils.GenericRecordUtils.entityResponseToAspectMap;
import static com.linkedin.metadata.utils.GenericRecordUtils.entityResponseToSystemAspectMap;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
public class EntityServiceAspectRetriever implements AspectRetriever {

  @Setter private OperationContext systemOperationContext;
  private final EntityRegistry entityRegistry;
  private final EntityService<?> entityService;

  @Nullable
  @Override
  public Aspect getLatestAspectObject(@Nonnull Urn urn, @Nonnull String aspectName) {
    return getLatestAspectObjects(Set.of(urn), Set.of(aspectName))
        .getOrDefault(urn, Map.of())
        .get(aspectName);
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, com.linkedin.entity.Aspect>> getLatestAspectObjects(
      Set<Urn> urns, Set<String> aspectNames) {
    if (urns.isEmpty() || aspectNames.isEmpty()) {
      return Map.of();
    } else {
      String entityName = urns.stream().findFirst().map(Urn::getEntityType).get();
      try {
        return entityResponseToAspectMap(
            entityService.getEntitiesV2(
                systemOperationContext, entityName, urns, aspectNames, false));
      } catch (URISyntaxException e) {
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
                  urn -> urn, urn -> entityService.exists(systemOperationContext, urn)));
    }
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, SystemAspect>> getLatestSystemAspects(
      Map<Urn, Set<String>> urnAspectNames) {
    if (urnAspectNames.isEmpty()) {
      return Map.of();
    } else {
      String entityName =
          urnAspectNames.keySet().stream().findFirst().map(Urn::getEntityType).get();
      try {
        // TODO - This causes over-fetching if not all aspects are required for every URN
        return entityResponseToSystemAspectMap(
            entityService.getEntitiesV2(
                systemOperationContext,
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
  }
}
