/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.models;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.models.annotation.TimeseriesFieldAnnotation;
import lombok.NonNull;
import lombok.Value;

@Value
public class TimeseriesFieldSpec implements FieldSpec {
  @NonNull PathSpec path;
  @NonNull TimeseriesFieldAnnotation timeseriesFieldAnnotation;
  @NonNull DataSchema pegasusSchema;

  public String getName() {
    return timeseriesFieldAnnotation.getStatName();
  }
}
