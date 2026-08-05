package io.datahubproject.openapi.v1.models.entities;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EntityCountsResponseDto {
  private List<EntityTypeCountDto> counts;
  private Long activeTotal;
  private Long softDeletedTotal;
  private Long totalCount;
  private List<String> requestedTypes;
  private long computedAtMillis;
  private boolean cacheHit;
}
