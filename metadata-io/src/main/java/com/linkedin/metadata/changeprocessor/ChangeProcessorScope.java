package com.linkedin.metadata.changeprocessor;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


@Retention(RetentionPolicy.RUNTIME)
public @interface ChangeProcessorScope {
  /**
   * The list of entity-aspect pairs that applies to this processor (case-insensitive).
   * For a specific aspect on a specific entity: "entityName/aspectName", e.g. "corpUser/corpUserInfo
   * For all aspects on an entity: "entityName/*", e.g. "corpUser/*"
   * For all aspects on all entities: "*&#47;*"
   * @return A string array of all the applicable entity aspect paths
   */
  String[] entityAspectNames();

  /**
   * Determines when the processor should be invoked. I.e. before or after the change is applied to the underlying
   * storage layer
   * @return Enum: PRE or POST
   */
  ChangeProcessorType processorType();
}

