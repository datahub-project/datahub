/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.entity;

import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import javax.annotation.Nonnull;

public interface SearchIndicesService {
  void handleChangeEvent(
      @Nonnull OperationContext opContext, @Nonnull MetadataChangeLog metadataChangeLog);

  void handleChangeEvents(
      @Nonnull OperationContext opContext, @Nonnull Collection<MetadataChangeLog> events);
}
