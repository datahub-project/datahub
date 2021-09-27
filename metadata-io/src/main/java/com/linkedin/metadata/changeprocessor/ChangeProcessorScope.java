package com.linkedin.metadata.changeprocessor;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


@Retention(RetentionPolicy.RUNTIME)
public @interface ChangeProcessorScope {
  /**
   * The list of entity-aspect pairs that applies to this processor (case insensitive). Must be in the form
   * "entityName:aspectName" e.g. "corpUser:corpUserInfo
   * @return A string array of all the applicable entity aspect pairs
   */
  String[] entityAspectNames();

  /**
   * Determines when the processor should be invoked. I.e. before or after the change is applied to the underlying
   * storage layer
   * @return Enum of BEFORE or AFTER
   */
  ChangeProcessorType processorType();
}

