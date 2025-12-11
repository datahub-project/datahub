/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.v2.models;

import io.datahubproject.openapi.models.GenericEntityScrollResult;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class GenericEntityScrollResultV2
    implements GenericEntityScrollResult<GenericAspectV2, GenericEntityV2> {
  private String scrollId;
  private List<GenericEntityV2> results;
}
