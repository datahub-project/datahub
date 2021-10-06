package com.linkedin.metadata.changeprocessor;

/**
 * Determines what to do after running the registered change processors.
 * SUCCESS -> Continues processing as normal
 * FAILURE -> Stops all processing of the change effectively ignoring it
 */
public enum ChangeState {
  SUCCESS, FAILURE
}
