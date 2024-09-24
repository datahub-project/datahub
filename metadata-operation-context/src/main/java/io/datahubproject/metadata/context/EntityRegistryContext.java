package io.datahubproject.metadata.context;

import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EmptyEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder
@Getter
@EqualsAndHashCode
public class EntityRegistryContext implements ContextInterface {
  public static final EntityRegistryContext EMPTY =
      EntityRegistryContext.builder().build(EmptyEntityRegistry.EMPTY);

  @EqualsAndHashCode.Exclude @Nonnull private final EntityRegistry entityRegistry;
  @Nonnull private final Map<String, Set<String>> entityToAspectsMap;

  public Set<String> getEntityAspectNames(String entityType) {
    return entityToAspectsMap.getOrDefault(entityType, Set.of());
  }

  protected Set<String> getEntityAspectNames(final Urn entityUrn) {
    return getEntityAspectNames(urnToEntityName(entityUrn));
  }

  public String getKeyAspectName(@Nonnull Urn urn) {
    final EntitySpec spec = entityRegistry.getEntitySpec(urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    return keySpec.getName();
  }

  public AspectSpec getKeyAspectSpec(@Nonnull final Urn urn) {
    return getKeyAspectSpec(urnToEntityName(urn));
  }

  public AspectSpec getKeyAspectSpec(@Nonnull final String entityName) {
    final EntitySpec spec = entityRegistry.getEntitySpec(entityName);
    return spec.getKeyAspectSpec();
  }

  public Optional<AspectSpec> getAspectSpec(
      @Nonnull final String entityName, @Nonnull final String aspectName) {
    final EntitySpec entitySpec = entityRegistry.getEntitySpec(entityName);
    return Optional.ofNullable(entitySpec.getAspectSpec(aspectName));
  }

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.ofNullable(entityRegistry.getIdentifier()).map(String::hashCode);
  }

  public static class EntityRegistryContextBuilder {
    private EntityRegistryContextBuilder build() {
      return null;
    }

    public EntityRegistryContext build(@Nonnull EntityRegistry entityRegistry) {
      return new EntityRegistryContext(entityRegistry, buildEntityToValidAspects(entityRegistry));
    }

    private static Map<String, Set<String>> buildEntityToValidAspects(
        final EntityRegistry entityRegistry) {
      return entityRegistry.getEntitySpecs().values().stream()
          .collect(
              Collectors.toMap(
                  EntitySpec::getName,
                  entry ->
                      entry.getAspectSpecs().stream()
                          .map(AspectSpec::getName)
                          .collect(Collectors.toSet())));
    }
  }
}
