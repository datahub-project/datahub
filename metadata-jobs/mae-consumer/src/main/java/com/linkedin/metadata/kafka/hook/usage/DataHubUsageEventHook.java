/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka.hook.usage;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.kafka.listener.EventHook;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

public interface DataHubUsageEventHook extends EventHook<JsonNode> {

  default DataHubUsageEventHook init(@Nonnull OperationContext systemOperationContext) {
    return this;
  }

  /** Invoke the hook when a DataHubUsageEvent is received */
  void invoke(@Nonnull JsonNode event) throws Exception;

  /**
   * Controls hook execution ordering
   *
   * @return order to execute
   */
  default int executionOrder() {
    return 100;
  }
}
