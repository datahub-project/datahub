package io.datahubproject.metadata.context;

import com.datahub.authorization.config.SearchAuthorizationConfiguration;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
public class OperationContextConfig implements ContextInterface {
  /**
   * Whether the given session authentication is allowed to assume the system authentication as
   * needed
   */
  private final boolean allowSystemAuthentication;

  /** Configuration for search authorization */
  private final SearchAuthorizationConfiguration searchAuthorizationConfiguration;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.of(searchAuthorizationConfiguration.hashCode());
  }
}
