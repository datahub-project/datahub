/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.timeline.eventgenerator;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.SystemMetadata;
import lombok.AllArgsConstructor;
import lombok.Value;

/** Thin wrapper for an aspect value which is used within the Entity Change Event API. */
@Value
@AllArgsConstructor
public class Aspect<T extends RecordTemplate> {
  /** The aspect value itself. */
  T value;

  /** System metadata */
  SystemMetadata systemMetadata;
}
