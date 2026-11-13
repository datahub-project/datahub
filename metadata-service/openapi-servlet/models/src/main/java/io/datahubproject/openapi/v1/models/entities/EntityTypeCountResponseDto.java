package io.datahubproject.openapi.v1.models.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EntityTypeCountResponseDto {
  private String entityType;
  private String keyAspect;
  private long activeCount;
  private long softDeletedCount;
  private Long totalCount;
  private long computedAtMillis;
  private boolean cacheHit;
}
