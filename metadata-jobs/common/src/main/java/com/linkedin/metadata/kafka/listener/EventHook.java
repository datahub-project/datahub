/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka.listener;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

public interface EventHook<E> {
  default EventHook<E> init(@Nonnull OperationContext systemOperationContext) {
    return this;
  }

  /**
   * Suffix for the consumer group
   *
   * @return suffix
   */
  @Nonnull
  String getConsumerGroupSuffix();

  /**
   * Return whether the hook is enabled or not. If not enabled, the below invoke method is not
   * triggered
   */
  boolean isEnabled();

  /** Invoke the hook when a MetadataChangeLog is received */
  void invoke(@Nonnull E event) throws Exception;

  /**
   * Controls hook execution ordering
   *
   * @return order to execute
   */
  default int executionOrder() {
    return 100;
  }
}
