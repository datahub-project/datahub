package io.datahubproject.openapi.v1.entities;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import io.datahubproject.openapi.v1.models.entities.EntityCountsResponseDto;
import io.datahubproject.openapi.v1.models.entities.EntityTypeCountResponseDto;
import java.time.Instant;
import java.util.List;
import org.testng.annotations.Test;

public class EntityCountsResponseMapperTest {

  @Test
  public void toBatchResponseOmitsTotalsWhenNotRequested() {
    KeyAspectEntityCountResult result = sampleBatchResult();

    EntityCountsResponseDto response = EntityCountsResponseMapper.toBatchResponse(result, false);

    assertEquals(response.getCounts().size(), 2);
    assertNull(response.getCounts().get(0).getTotalCount());
    assertNull(response.getActiveTotal());
    assertNull(response.getSoftDeletedTotal());
    assertNull(response.getTotalCount());
    assertEquals(response.getRequestedTypes(), List.of("chart", "dataset"));
    assertEquals(response.isCacheHit(), true);
    assertEquals(
        response.getComputedAtMillis(), Instant.parse("2026-07-07T12:00:00Z").toEpochMilli());
  }

  @Test
  public void toBatchResponseIncludesTotalsWhenRequested() {
    KeyAspectEntityCountResult result = sampleBatchResult();

    EntityCountsResponseDto response = EntityCountsResponseMapper.toBatchResponse(result, true);

    assertEquals(response.getCounts().get(0).getTotalCount(), Long.valueOf(3L));
    assertEquals(response.getCounts().get(1).getTotalCount(), Long.valueOf(7L));
    assertEquals(response.getActiveTotal(), Long.valueOf(8L));
    assertEquals(response.getSoftDeletedTotal(), Long.valueOf(2L));
    assertEquals(response.getTotalCount(), Long.valueOf(10L));
  }

  @Test
  public void toSingleResponseIncludesTotalWhenRequested() {
    KeyAspectEntityCountResult result =
        KeyAspectEntityCountResult.builder()
            .counts(
                List.of(
                    KeyAspectEntityCountEntry.builder()
                        .entityType("dataset")
                        .keyAspect("datasetKey")
                        .activeCount(6L)
                        .softDeletedCount(2L)
                        .build()))
            .requestedTypes(List.of("dataset"))
            .computedAt(Instant.parse("2026-07-07T12:00:00Z"))
            .cacheHit(false)
            .build();

    EntityTypeCountResponseDto response = EntityCountsResponseMapper.toSingleResponse(result, true);

    assertEquals(response.getEntityType(), "dataset");
    assertEquals(response.getTotalCount(), Long.valueOf(8L));
    assertEquals(response.isCacheHit(), false);
  }

  @Test
  public void toSingleResponseOmitsTotalWhenNotRequested() {
    KeyAspectEntityCountResult result =
        KeyAspectEntityCountResult.builder()
            .counts(
                List.of(
                    KeyAspectEntityCountEntry.builder()
                        .entityType("dataset")
                        .keyAspect("datasetKey")
                        .activeCount(6L)
                        .softDeletedCount(2L)
                        .build()))
            .requestedTypes(List.of("dataset"))
            .computedAt(Instant.parse("2026-07-07T12:00:00Z"))
            .cacheHit(false)
            .build();

    EntityTypeCountResponseDto response =
        EntityCountsResponseMapper.toSingleResponse(result, false);

    assertNull(response.getTotalCount());
  }

  private static KeyAspectEntityCountResult sampleBatchResult() {
    return KeyAspectEntityCountResult.builder()
        .counts(
            List.of(
                KeyAspectEntityCountEntry.builder()
                    .entityType("chart")
                    .keyAspect("chartKey")
                    .activeCount(2L)
                    .softDeletedCount(1L)
                    .build(),
                KeyAspectEntityCountEntry.builder()
                    .entityType("dataset")
                    .keyAspect("datasetKey")
                    .activeCount(6L)
                    .softDeletedCount(1L)
                    .build()))
        .requestedTypes(List.of("chart", "dataset"))
        .computedAt(Instant.parse("2026-07-07T12:00:00Z"))
        .cacheHit(true)
        .build();
  }
}
