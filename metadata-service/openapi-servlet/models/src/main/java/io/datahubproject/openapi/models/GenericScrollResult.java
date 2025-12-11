/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.models;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GenericScrollResult<T> {
  private String scrollId;
  private List<T> results;
}
