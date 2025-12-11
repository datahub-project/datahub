/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
