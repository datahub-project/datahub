package com.linkedin.metadata.systemmetadata;

import static org.testng.Assert.assertEquals;

import java.util.List;
import org.testng.annotations.Test;

public class KeyAspectCountModelsTest {

  @Test
  public void keyAspectCountTotals() {
    KeyAspectCount count = KeyAspectCount.builder().activeCount(3L).softDeletedCount(2L).build();

    assertEquals(count.getActiveCount(), 3L);
    assertEquals(count.getSoftDeletedCount(), 2L);
    assertEquals(count.totalCount(), 5L);
    assertEquals(KeyAspectCount.empty().totalCount(), 0L);
  }

  @Test
  public void keyAspectEntityCountEntryOfCopiesCounts() {
    KeyAspectEntityCountEntry entry =
        KeyAspectEntityCountEntry.of(
            "dataset",
            "datasetKey",
            KeyAspectCount.builder().activeCount(4L).softDeletedCount(1L).build());

    assertEquals(entry.getEntityType(), "dataset");
    assertEquals(entry.getKeyAspect(), "datasetKey");
    assertEquals(entry.totalCount(), 5L);
  }

  @Test
  public void keyAspectEntityCountResultRollups() {
    KeyAspectEntityCountResult result =
        KeyAspectEntityCountResult.builder()
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
                        .activeCount(5L)
                        .softDeletedCount(0L)
                        .build()))
            .requestedTypes(List.of("chart", "dataset"))
            .computedAt(java.time.Instant.now())
            .cacheHit(false)
            .build();

    assertEquals(result.activeTotal(), 7L);
    assertEquals(result.softDeletedTotal(), 1L);
    assertEquals(result.totalCount(), 8L);
  }
}
