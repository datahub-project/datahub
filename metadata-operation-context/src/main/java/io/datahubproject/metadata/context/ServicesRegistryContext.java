/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
