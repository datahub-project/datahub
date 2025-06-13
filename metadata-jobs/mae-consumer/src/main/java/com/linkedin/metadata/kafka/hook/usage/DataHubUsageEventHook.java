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
