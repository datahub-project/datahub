package com.linkedin.metadata.kafka.usage;

import com.linkedin.metadata.kafka.generic.EventHook;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface DataHubUsageEventHook extends EventHook<ConsumerRecord<String, String>> {

  default DataHubUsageEventHook init(@Nonnull OperationContext systemOperationContext) {
    return this;
  }

  /** Invoke the hook when a MetadataChangeLog is received */
  void invoke(@Nonnull ConsumerRecord<String, String> event) throws Exception;

  /**
   * Controls hook execution ordering
   *
   * @return order to execute
   */
  default int executionOrder() {
    return 100;
  }
}
