package com.linkedin.metadata.systemmetadata.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import java.time.Instant;
import java.util.List;
import org.testng.annotations.Test;

public class EntityCountMetricsSinkComposerTest {

  @Test
  public void publishDelegatesToAllSinks() {
    RecordingEntityCountMetricsSink first = new RecordingEntityCountMetricsSink();
    RecordingEntityCountMetricsSink second = new RecordingEntityCountMetricsSink();
    EntityCountMetricsSinkComposer composer =
        new EntityCountMetricsSinkComposer(List.of(first, second));
    KeyAspectEntityCountResult result = sampleResult();

    composer.publish(result);

    assertEquals(first.results(), List.of(result));
    assertEquals(second.results(), List.of(result));
  }

  @Test
  public void publishContinuesAfterSinkFailureAndRethrowsLastFailure() {
    RecordingEntityCountMetricsSink recordingSink = new RecordingEntityCountMetricsSink();
    EntityCountMetricsSinkComposer composer =
        new EntityCountMetricsSinkComposer(
            List.of(
                recordingSink,
                result -> {
                  throw new RuntimeException("sink failed");
                }));
    KeyAspectEntityCountResult result = sampleResult();

    assertThrows(RuntimeException.class, () -> composer.publish(result));
    assertEquals(recordingSink.results(), List.of(result));
  }

  private static KeyAspectEntityCountResult sampleResult() {
    return KeyAspectEntityCountResult.builder()
        .counts(
            List.of(
                KeyAspectEntityCountEntry.builder()
                    .entityType("dataset")
                    .keyAspect("datasetKey")
                    .activeCount(10)
                    .softDeletedCount(1)
                    .build()))
        .requestedTypes(List.of("dataset"))
        .computedAt(Instant.now())
        .cacheHit(false)
        .build();
  }
}
