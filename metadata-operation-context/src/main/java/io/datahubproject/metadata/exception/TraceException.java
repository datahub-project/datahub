/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.metadata.exception;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
@AllArgsConstructor
@JsonInclude(Include.NON_NULL)
public class TraceException {
  String message;
  String exceptionClass;
  String[] stackTrace;
  TraceException cause;

  public TraceException(Throwable throwable) {
    this.message = throwable.getMessage();
    this.exceptionClass = throwable.getClass().getName();
    this.stackTrace =
        Arrays.stream(throwable.getStackTrace())
            .map(StackTraceElement::toString)
            .toArray(String[]::new);

    // Handle nested cause
    this.cause = throwable.getCause() != null ? new TraceException(throwable.getCause()) : null;
  }

  public TraceException(String message) {
    this.message = message;
    this.exceptionClass = null;
    this.stackTrace = null;
    this.cause = null;
  }
}
