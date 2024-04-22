package io.datahubproject.metadata.context;

import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class EntityRegistryContext implements ContextInterface {
  public static EntityRegistryContext EMPTY = EntityRegistryContext.builder().build();

  @Nullable private final EntityRegistry entityRegistry;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return entityRegistry == null
        ? Optional.empty()
        : Optional.ofNullable(entityRegistry.getIdentifier()).map(String::hashCode);
  }
}
