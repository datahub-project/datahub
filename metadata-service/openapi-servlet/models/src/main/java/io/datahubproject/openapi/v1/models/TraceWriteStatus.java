/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
