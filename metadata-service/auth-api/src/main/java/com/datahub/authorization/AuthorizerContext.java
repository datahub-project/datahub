package com.datahub.authorization;

import lombok.AllArgsConstructor;
import lombok.Data;


/**
 * Context provided to an Authorizer on initialization.
 */
@Data
@AllArgsConstructor
public class AuthorizerContext {
  /**
   * A utility for resolving a {@link ResourceSpec} to resolved resource field values.
   */
  private ResourceSpecResolver resourceSpecResolver;
}
