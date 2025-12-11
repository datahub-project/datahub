/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.systemmetadata;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.v1.models.TraceStatus;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public interface TraceService {

  @Nonnull
  Map<Urn, Map<String, TraceStatus>> trace(
      @Nonnull OperationContext opContext,
      @Nonnull String traceId,
      @Nonnull Map<Urn, List<String>> aspects,
      boolean onlyIncludeErrors,
      boolean detailed,
      boolean skipCache);

  @Nonnull
  default Map<Urn, Map<String, TraceStatus>> trace(
      @Nonnull OperationContext opContext,
      @Nonnull String traceId,
      @Nonnull Map<Urn, List<String>> aspects,
      boolean onlyIncludeErrors,
      boolean detailed) {
    return trace(opContext, traceId, aspects, onlyIncludeErrors, detailed, false);
  }

  @Nonnull
  default Map<Urn, Map<String, TraceStatus>> traceDetailed(
      @Nonnull OperationContext opContext,
      @Nonnull String traceId,
      @Nonnull Map<Urn, List<String>> aspects,
      boolean skipCache) {
    return trace(opContext, traceId, aspects, false, true, skipCache);
  }
}
