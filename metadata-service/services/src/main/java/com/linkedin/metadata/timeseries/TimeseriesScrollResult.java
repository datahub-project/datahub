/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.timeseries;

import com.linkedin.metadata.aspect.EnvelopedAspect;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@AllArgsConstructor
@Data
@Builder
public class TimeseriesScrollResult {
  int numResults;
  int pageSize;
  String scrollId;
  List<EnvelopedAspect> events;
  List<GenericTimeseriesDocument> documents;
}
