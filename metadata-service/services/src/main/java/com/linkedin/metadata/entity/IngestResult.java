/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.batch.BatchItem;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class IngestResult {
  Urn urn;
  BatchItem request;
  @Nullable UpdateAspectResult result;
  boolean publishedMCL;
  boolean processedMCL;
  boolean publishedMCP;
  boolean sqlCommitted;
  boolean isUpdate; // update else insert
}
