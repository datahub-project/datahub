package com.linkedin.metadata.systemmetadata.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountEntry;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import com.linkedin.metadata.systemmetadata.PlatformEntityCountEntry;
import com.linkedin.metadata.systemmetadata.PlatformEntityCountResult;
import java.time.Instant;
import java.util.List;
import javax.annotation.Nonnull;
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

  @Test
  public void publishPlatformDelegatesToAllSinks() {
    RecordingEntityCountMetricsSink first = new RecordingEntityCountMetricsSink();
    RecordingEntityCountMetricsSink second = new RecordingEntityCountMetricsSink();
    EntityCountMetricsSinkComposer composer =
        new EntityCountMetricsSinkComposer(List.of(first, second));
    PlatformEntityCountResult result = samplePlatformResult();

    composer.publishPlatform(result);

    assertEquals(first.platformResults(), List.of(result));
    assertEquals(second.platformResults(), List.of(result));
  }

  @Test
  public void publishPlatformContinuesAfterSinkFailureAndRethrowsLastFailure() {
    RecordingEntityCountMetricsSink recordingSink = new RecordingEntityCountMetricsSink();
    EntityCountMetricsSinkComposer composer =
        new EntityCountMetricsSinkComposer(
            List.of(
                recordingSink,
                new EntityCountMetricsSink() {
                  @Override
                  public void publish(@Nonnull KeyAspectEntityCountResult ignored) {}

                  @Override
                  public void publishPlatform(@Nonnull PlatformEntityCountResult ignored) {
                    throw new RuntimeException("platform sink failed");
                  }
                }));
    PlatformEntityCountResult result = samplePlatformResult();

    assertThrows(RuntimeException.class, () -> composer.publishPlatform(result));
    assertEquals(recordingSink.platformResults(), List.of(result));
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

  private static PlatformEntityCountResult samplePlatformResult() {
    return PlatformEntityCountResult.builder()
        .counts(
            List.of(
                PlatformEntityCountEntry.builder()
                    .entityType("dataset")
                    .platform("snowflake")
                    .activeCount(6)
                    .softDeletedCount(1)
                    .build()))
        .requestedTypes(List.of("dataset"))
        .computedAt(Instant.parse("2026-02-01T23:00:00Z"))
        .build();
  }
}
