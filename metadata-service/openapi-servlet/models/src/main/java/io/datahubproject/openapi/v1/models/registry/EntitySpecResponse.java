/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.v1.models.registry;

import com.linkedin.metadata.models.EntitySpec;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EntitySpecResponse {
  private String name;
  private List<AspectSpecDto> aspectSpecs;

  public static EntitySpecResponse fromEntitySpec(EntitySpec entitySpec) {
    return EntitySpecResponse.builder()
        .name(entitySpec.getName())
        .aspectSpecs(
            entitySpec.getAspectSpecs().stream()
                .map(AspectSpecDto::fromAspectSpec)
                .collect(Collectors.toList()))
        .build();
  }
}
