package com.linkedin.metadata.entity;

import static com.linkedin.metadata.utils.GenericRecordUtils.entityResponseToAspectMap;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
public class EntityServiceAspectRetriever implements CachingAspectRetriever {

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
            entityService.getEntitiesV2(systemOperationContext, entityName, urns, aspectNames));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
