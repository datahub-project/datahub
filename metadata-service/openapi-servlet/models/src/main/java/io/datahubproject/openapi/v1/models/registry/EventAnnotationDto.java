/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.v1.models.registry;

import com.linkedin.metadata.models.annotation.EventAnnotation;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EventAnnotationDto {
  private String name;

  public static EventAnnotationDto fromEventAnnotation(EventAnnotation annotation) {
    if (annotation == null) {
      return null;
    }

    return EventAnnotationDto.builder().name(annotation.getName()).build();
  }
}
