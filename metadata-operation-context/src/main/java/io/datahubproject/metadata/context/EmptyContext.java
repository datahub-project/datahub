/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.metadata.context;

import java.util.Optional;

public class EmptyContext implements ContextInterface {
  public static final EmptyContext EMPTY = new EmptyContext();

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }
}
