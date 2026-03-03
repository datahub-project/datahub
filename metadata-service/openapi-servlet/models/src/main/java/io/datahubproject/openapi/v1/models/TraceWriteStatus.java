package io.datahubproject.openapi.v1.models;

public enum TraceWriteStatus {
  // error occurred during processing
  ERROR,
  // write is queued
  PENDING,
  // write is the active value in the datastore
  ACTIVE_STATE,
  // write has been overwritten with a newer value.
  HISTORIC_STATE,
  // write is not required
  NO_OP,
  // Unknown status due to the fact that tracing is lost or potentially well outside the expected
  // tracing range (i.e. last year)
  UNKNOWN,
  TRACE_NOT_IMPLEMENTED
}
