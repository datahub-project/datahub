package io.datahubproject.metadata.context;

import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import io.datahubproject.metadata.context.usage.instrumentation.SessionContextEnricher;
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

  /** Optional usage-metrics instrumentation hook for {@link OperationContext#asSession}. */
  @Builder.Default private final SessionContextEnricher sessionContextEnricher = null;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    int hash = viewAuthorizationConfiguration.hashCode();
    if (sessionContextEnricher != null) {
      hash = 31 * hash + sessionContextEnricher.getClass().hashCode();
    }
    return Optional.of(hash);
  }
}
