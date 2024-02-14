package io.datahubproject.metadata.context;

import com.datahub.authorization.config.SearchAuthorizationConfiguration;
import lombok.Builder;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
public class OperationContextConfig {
  /**
   * Whether the given session authentication is allowed to assume the system authentication as
   * needed
   */
  private final boolean allowSystemAuthentication;

  /** Configuration for search authorization */
  private final SearchAuthorizationConfiguration searchAuthorizationConfiguration;
}
