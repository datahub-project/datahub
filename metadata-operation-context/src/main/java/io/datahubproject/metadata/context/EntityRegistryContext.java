package io.datahubproject.metadata.context;

import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class EntityRegistryContext implements ContextInterface {
  @Nonnull private final EntityRegistry entityRegistry;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.of(entityRegistry.getIdentifier().hashCode());
  }
}
