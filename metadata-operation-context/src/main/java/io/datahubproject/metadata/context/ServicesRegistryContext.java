package io.datahubproject.metadata.context;

import io.datahubproject.metadata.services.RestrictedService;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ServicesRegistryContext implements ContextInterface {

  @Nonnull private final RestrictedService restrictedService;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }
}
