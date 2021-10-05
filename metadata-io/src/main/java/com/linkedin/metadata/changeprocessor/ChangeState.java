package com.linkedin.metadata.changeprocessor;

/**
 * Determines what to do after running the registered change processors.
 * SUCCESS -> Continues processing as normal
 * BLOCKER -> Persists the change to the document store but does run after change processors or emit a mae event.
 * FAILURE -> Stops all processing of the change
 */
public enum ChangeState {
  SUCCESS, BLOCKER, FAILURE
}
