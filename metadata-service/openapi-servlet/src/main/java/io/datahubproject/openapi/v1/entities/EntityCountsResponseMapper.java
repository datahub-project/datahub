package io.datahubproject.openapi.v1.entities;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import io.datahubproject.openapi.v1.models.entities.EntityCountsResponseDto;
import io.datahubproject.openapi.v1.models.entities.EntityTypeCountDto;
import io.datahubproject.openapi.v1.models.entities.EntityTypeCountResponseDto;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

final class EntityCountsResponseMapper {

  private EntityCountsResponseMapper() {}

  @Nonnull
  static EntityCountsResponseDto toBatchResponse(
      @Nonnull KeyAspectEntityCountResult result, boolean includeTotal) {
    List<EntityTypeCountDto> counts =
        result.getCounts().stream()
            .map(entry -> toEntityTypeCountDto(entry, includeTotal))
            .collect(Collectors.toList());

    EntityCountsResponseDto.EntityCountsResponseDtoBuilder builder =
        EntityCountsResponseDto.builder()
            .counts(counts)
            .requestedTypes(result.getRequestedTypes())
            .computedAt(result.getComputedAt())
            .cacheHit(result.isCacheHit());

    if (includeTotal) {
      builder
          .activeTotal(result.activeTotal())
          .softDeletedTotal(result.softDeletedTotal())
          .totalCount(result.totalCount());
    }

    return builder.build();
  }

  @Nonnull
  static EntityTypeCountResponseDto toSingleResponse(
      @Nonnull KeyAspectEntityCountResult result, boolean includeTotal) {
    KeyAspectEntityCountEntry entry = result.getCounts().get(0);
    EntityTypeCountResponseDto.EntityTypeCountResponseDtoBuilder builder =
        EntityTypeCountResponseDto.builder()
            .entityType(entry.getEntityType())
            .keyAspect(entry.getKeyAspect())
            .activeCount(entry.getActiveCount())
            .softDeletedCount(entry.getSoftDeletedCount())
            .computedAt(result.getComputedAt())
            .cacheHit(result.isCacheHit());

    if (includeTotal) {
      builder.totalCount(entry.totalCount());
    }

    return builder.build();
  }

  @Nonnull
  private static EntityTypeCountDto toEntityTypeCountDto(
      @Nonnull KeyAspectEntityCountEntry entry, boolean includeTotal) {
    EntityTypeCountDto.EntityTypeCountDtoBuilder builder =
        EntityTypeCountDto.builder()
            .entityType(entry.getEntityType())
            .keyAspect(entry.getKeyAspect())
            .activeCount(entry.getActiveCount())
            .softDeletedCount(entry.getSoftDeletedCount());

    if (includeTotal) {
      builder.totalCount(entry.totalCount());
    }

    return builder.build();
  }
}
