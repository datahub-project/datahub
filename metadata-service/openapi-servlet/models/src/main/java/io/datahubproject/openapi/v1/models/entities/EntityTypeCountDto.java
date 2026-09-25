package io.datahubproject.openapi.v1.models.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EntityTypeCountDto {
  private String entityType;
  private String keyAspect;

  /** Present when {@code groupBy=platform}; absent/null for type-only counts. */
  private String platform;

  private long activeCount;
  private long softDeletedCount;
  private Long totalCount;
}
