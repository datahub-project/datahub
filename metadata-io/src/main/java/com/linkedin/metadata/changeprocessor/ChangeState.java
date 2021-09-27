package com.linkedin.metadata.changeprocessor;

/**
 * Determines what to do after running the registered change processors.
 * CONTINUE -> Continues processing as normal
 * SAVE_THEN_STOP -> Persists the change to the document store but does run after change processors or emit a mae event.
 * STOP_PROCESSING -> Stops all processing of the change
 */
public enum ChangeState {
  CONTINUE, SAVE_THEN_STOP, STOP_PROCESSING
}
