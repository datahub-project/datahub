package com.datahub.plugins.auth.authorization;

import java.util.Map;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Data;


/**
 * Context provided to an Authorizer on initialization.
 */
@Data
@AllArgsConstructor
public class AuthorizerContext {
  private final Map<String, Object> contextMap;

  /**
   * A utility for resolving a {@link ResourceSpec} to resolved resource field values.
   */
  private ResourceSpecResolver resourceSpecResolver;

  @Nonnull
  public Map<String, Object> data() {
    return contextMap;
  }
}
