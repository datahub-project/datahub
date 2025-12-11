/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.datahubproject.openapi.generated.AspectRowSummary;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RollbackRunResultDto {
  List<AspectRowSummary> rowsRolledBack;
  Integer rowsDeletedFromEntityDeletion;
}
