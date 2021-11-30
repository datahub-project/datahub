package com.linkedin.metadata.kafka.hook;

import com.linkedin.mxe.MetadataChangeLog;


/**
 * Custom hook which is invoked on receiving a new {@link MetadataChangeLog} event.
 */
public interface MetadataChangeLogHook {

  /**
   * Initialize the hook
   */
  default void init() { }

  /**
   * Invoke the hook when a MetadataChangeLog is received
   */
  void invoke(MetadataChangeLog log);

}
