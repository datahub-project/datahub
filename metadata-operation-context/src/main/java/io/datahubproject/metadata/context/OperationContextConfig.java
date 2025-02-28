package io.datahubproject.metadata.context;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import java.util.Optional;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
public class OperationContextConfig implements ContextInterface {
  /**
   * Whether the given session authentication is allowed to assume the system authentication as
   * needed
   */
  private final boolean allowSystemAuthentication;

  /** Configuration for search authorization */
  private final ViewAuthorizationConfiguration viewAuthorizationConfiguration;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.of(viewAuthorizationConfiguration.hashCode());
  }
}
