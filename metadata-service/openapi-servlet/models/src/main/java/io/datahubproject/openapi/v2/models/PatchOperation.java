/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.v2.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PatchOperation {
  @Nonnull private String op;
  @Nonnull private String path;
  @Nullable private JsonNode value;
  @Nullable private List<String> arrayMapKey;

  public PatchOperationType getOp() {
    return PatchOperationType.valueOf(op.toUpperCase());
  }
}
